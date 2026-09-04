use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};

use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata};
use crossbeam_channel::{Receiver, Sender};

use log::{debug, error, info, warn};

use super::{S3ByteStream, S3Client, S3Request, S3Result};
use crate::Result;

fn format_s3_error<E>(operation: &str, key: &str, err: &E, status: Option<u16>) -> String
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    let mut msg = format!("{operation} failed for key '{key}': {err}");
    if let Some(status) = status {
        msg.push_str(&format!(" (status={status})"));
    }
    let meta = err.meta();
    if let Some(code) = meta.code() {
        msg.push_str(&format!(" code={code}"));
    }
    if let Some(message) = meta.message() {
        msg.push_str(&format!(" message={message:?}"));
    }
    msg.push_str(&format!(" — {}", DisplayErrorContext(err)));
    msg
}

struct WorkerContext {
    client: S3Client,
    bucket: Arc<String>,
}

pub(super) fn spawn_workers(
    client: S3Client,
    bucket: Arc<String>,
    mut worker_threads: usize,
    request_rx: Receiver<S3Request>,
    result_tx: Sender<S3Result>,
) -> Result<Vec<JoinHandle<()>>> {
    let client_config = client.config().clone();

    if worker_threads < 1 {
        worker_threads = 1;
        warn!("At least one S3 worker thread is required; defaulting to 1");
    }
    if worker_threads > 128 {
        worker_threads = 128;
        warn!("Capping S3 worker threads to maximum of 128");
    }

    let mut workers = Vec::with_capacity(worker_threads);

    for i in 0..worker_threads {
        let ctx = WorkerContext {
            client: S3Client::from_conf(client_config.clone()),
            bucket: Arc::clone(&bucket),
        };
        let rx = request_rx.clone();
        let tx = result_tx.clone();

        let join_handle = thread::Builder::new()
            .name(format!("s3-worker-{}", i))
            .spawn(move || run_worker_loop(ctx, rx, tx, i))?;
        workers.push(join_handle);
    }

    Ok(workers)
}

fn run_worker_loop(ctx: WorkerContext, rx: Receiver<S3Request>, tx: Sender<S3Result>, id: usize) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!("Critical: Failed to build Tokio runtime for S3 worker: {e}");
            return;
        }
    };

    while let Ok(req) = rx.recv() {
        runtime.block_on(async {
            process_request(&ctx, req, &tx).await;
        });
    }

    info!("S3 worker thread {} exiting", id);
}

async fn process_request(ctx: &WorkerContext, req: S3Request, tx: &Sender<S3Result>) {
    match req {
        S3Request::Put { name, key, data } => {
            debug!("Uploading object to S3: {}", name);
            let result = upload_object(ctx, &key, data).await;
            if let Err(e) = tx.send(S3Result::Put {
                name: name.clone(),
                result,
            }) {
                error!("Failed to send S3Result::Put for {}: {}", name, e);
            }
        }
        S3Request::Get { name, key } => {
            debug!("Fetching object from S3: {}", name);
            let result = fetch_object(ctx, &key).await;
            if let Err(e) = tx.send(S3Result::Get {
                name: name.clone(),
                result,
            }) {
                error!("Failed to send S3Result::Get for {}: {}", name, e);
            }
        }
        S3Request::Delete { keys, reply } => {
            debug!("Deleting {} objects from S3", keys.len());
            let result = delete_objects(ctx, &keys).await;
            if reply.send(result).is_err() {
                error!("Failed to send DeleteObjects result: caller is gone");
            }
        }
    }
}

/// One `DeleteObjects` call in quiet mode, so the reply only lists the keys
/// that failed. Any such key fails the whole batch: the caller is an offline
/// tool that must not report a partial purge as complete.
async fn delete_objects(ctx: &WorkerContext, keys: &[String]) -> Result<()> {
    let describe = || {
        format!(
            "{} keys from '{}'",
            keys.len(),
            keys.first().map(String::as_str).unwrap_or("")
        )
    };
    let build_error = |err: aws_sdk_s3::error::BuildError| {
        crate::ubiblk_error!(ArchiveError {
            description: format!("Failed to build DeleteObjects request: {err}"),
        })
    };

    let objects = keys
        .iter()
        .map(|key| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(key)
                .build()
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(build_error)?;
    let delete = aws_sdk_s3::types::Delete::builder()
        .set_objects(Some(objects))
        .quiet(true)
        .build()
        .map_err(build_error)?;

    let output = ctx
        .client
        .delete_objects()
        .bucket(ctx.bucket.as_str())
        .delete(delete)
        .send()
        .await
        .map_err(|err| {
            let status = err.raw_response().map(|r| r.status().as_u16());
            crate::ubiblk_error!(ArchiveError {
                description: format_s3_error("DeleteObjects", &describe(), &err, status),
            })
        })?;

    let failed: Vec<String> = output
        .errors()
        .iter()
        .map(|e| {
            format!(
                "'{}': {} {}",
                e.key().unwrap_or("?"),
                e.code().unwrap_or("?"),
                e.message().unwrap_or("")
            )
        })
        .collect();
    if failed.is_empty() {
        Ok(())
    } else {
        Err(crate::ubiblk_error!(ArchiveError {
            description: format!(
                "DeleteObjects left {} of {} keys undeleted: {}",
                failed.len(),
                keys.len(),
                failed.join("; ")
            ),
        }))
    }
}

async fn upload_object(ctx: &WorkerContext, key: &str, data: Vec<u8>) -> Result<()> {
    ctx.client
        .put_object()
        .bucket(ctx.bucket.as_str())
        .key(key)
        .body(S3ByteStream::from(data))
        .send()
        .await
        .map_err(|err| {
            let status = err.raw_response().map(|r| r.status().as_u16());
            crate::ubiblk_error!(ArchiveError {
                description: format_s3_error("PutObject", key, &err, status),
            })
        })?;
    Ok(())
}

async fn fetch_object(ctx: &WorkerContext, key: &str) -> Result<Vec<u8>> {
    let output = ctx
        .client
        .get_object()
        .bucket(ctx.bucket.as_str())
        .key(key)
        .send()
        .await
        .map_err(|err| {
            let status = err.raw_response().map(|r| r.status().as_u16());
            crate::ubiblk_error!(ArchiveError {
                description: format_s3_error("GetObject", key, &err, status),
            })
        })?;

    let bytes = output.body.collect().await.map_err(|err| {
        crate::ubiblk_error!(ArchiveError {
            description: format!("Failed to read object body: {err}"),
        })
    })?;

    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::{
        delete_objects::{DeleteObjectsError, DeleteObjectsOutput},
        get_object::GetObjectOutput,
        put_object::PutObjectOutput,
    };
    use aws_smithy_mocks::{mock, mock_client, Rule};
    use crossbeam_channel::{bounded, unbounded};

    use super::*;

    #[derive(Debug)]
    struct FakeServiceError {
        meta: ErrorMetadata,
    }

    impl std::fmt::Display for FakeServiceError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("fake service error")
        }
    }

    impl std::error::Error for FakeServiceError {}

    impl ProvideErrorMetadata for FakeServiceError {
        fn meta(&self) -> &ErrorMetadata {
            &self.meta
        }
    }

    #[test]
    fn format_s3_error_includes_status_code_and_message() {
        let err = FakeServiceError {
            meta: ErrorMetadata::builder()
                .code("SlowDown")
                .message("Please reduce your request rate.")
                .build(),
        };
        let msg = format_s3_error("PutObject", "data/abc", &err, Some(503));
        assert!(msg.contains("PutObject"), "missing op: {msg}");
        assert!(msg.contains("data/abc"), "missing key: {msg}");
        assert!(msg.contains("status=503"), "missing status: {msg}");
        assert!(msg.contains("code=SlowDown"), "missing code: {msg}");
        assert!(
            msg.contains("Please reduce your request rate."),
            "missing message: {msg}"
        );
    }

    #[test]
    fn format_s3_error_handles_missing_metadata() {
        let err = FakeServiceError {
            meta: ErrorMetadata::builder().build(),
        };
        let msg = format_s3_error("GetObject", "metadata.json", &err, None);
        assert!(msg.contains("GetObject"), "missing op: {msg}");
        assert!(msg.contains("metadata.json"), "missing key: {msg}");
        assert!(!msg.contains("status="), "should omit status: {msg}");
        assert!(!msg.contains("code="), "should omit code: {msg}");
    }

    fn spawn_test_workers(
        rules: &[Rule],
    ) -> (Sender<S3Request>, Receiver<S3Result>, Vec<JoinHandle<()>>) {
        let (request_tx, request_rx) = unbounded();
        let (result_tx, result_rx) = unbounded();
        let workers = spawn_workers(
            mock_client!(aws_sdk_s3, rules),
            Arc::new("test-bucket".to_string()),
            1,
            request_rx,
            result_tx,
        )
        .expect("failed to spawn workers");
        (request_tx, result_rx, workers)
    }

    fn join_workers(workers: Vec<JoinHandle<()>>) {
        for worker in workers {
            let _ = worker.join();
        }
    }

    #[test]
    fn test_worker_put_and_get() {
        let put_rule =
            mock!(S3Client::put_object).then_output(|| PutObjectOutput::builder().build());
        let get_rule = mock!(S3Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .body(S3ByteStream::from_static(b"hello"))
                .build()
        });

        let (request_tx, result_rx, workers) = spawn_test_workers(&[put_rule, get_rule]);

        request_tx
            .send(S3Request::Put {
                name: "obj-put".to_string(),
                key: "prefix/obj-put".to_string(),
                data: b"payload".to_vec(),
            })
            .expect("failed to send put request");
        request_tx
            .send(S3Request::Get {
                name: "obj-get".to_string(),
                key: "prefix/obj-get".to_string(),
            })
            .expect("failed to send get request");

        let first = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("missing first result");
        let second = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("missing second result");

        let mut results = [first, second];
        results.sort_by_key(|result| match result {
            S3Result::Put { name, .. } => name.clone(),
            S3Result::Get { name, .. } => name.clone(),
        });

        match &results[0] {
            S3Result::Get { name, result } => {
                assert_eq!(name, "obj-get");
                assert_eq!(result.as_ref().unwrap(), b"hello");
            }
            _ => panic!("expected get result first after sort"),
        }

        match &results[1] {
            S3Result::Put { name, result } => {
                assert_eq!(name, "obj-put");
                assert!(result.is_ok());
            }
            _ => panic!("expected put result second after sort"),
        }

        drop(request_tx);
        join_workers(workers);
    }

    fn worker_delete(rules: &[Rule], keys: &[&str]) -> Result<()> {
        let (request_tx, _result_rx, workers) = spawn_test_workers(rules);
        let (reply_tx, reply_rx) = bounded(1);
        request_tx
            .send(S3Request::Delete {
                keys: keys.iter().map(|k| k.to_string()).collect(),
                reply: reply_tx,
            })
            .expect("failed to send delete request");
        let result = reply_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("missing delete reply");
        drop(request_tx);
        join_workers(workers);
        result
    }

    #[test]
    fn test_worker_delete_replies_on_its_own_channel() {
        let rule = mock!(S3Client::delete_objects)
            .match_requests(|req| {
                let delete = req.delete().expect("delete body");
                delete.quiet() == Some(true)
                    && delete.objects().iter().map(|o| o.key()).collect::<Vec<_>>()
                        == ["prefix/dev/0", "prefix/dev/1"]
            })
            .then_output(|| DeleteObjectsOutput::builder().build());

        worker_delete(&[rule], &["prefix/dev/0", "prefix/dev/1"]).expect("delete succeeds");
    }

    #[test]
    fn test_worker_delete_reports_service_error() {
        let rule = mock!(S3Client::delete_objects).then_error(|| {
            DeleteObjectsError::generic(
                ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("Access Denied")
                    .build(),
            )
        });

        let err = worker_delete(&[rule], &["prefix/dev/0"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("DeleteObjects failed"), "{err}");
        assert!(err.contains("1 keys from 'prefix/dev/0'"), "{err}");
        assert!(err.contains("code=AccessDenied"), "{err}");
    }

    #[test]
    fn test_worker_delete_fails_the_batch_on_a_failed_key() {
        let rule = mock!(S3Client::delete_objects).then_output(|| {
            DeleteObjectsOutput::builder()
                .errors(
                    aws_sdk_s3::types::Error::builder()
                        .key("prefix/dev/1")
                        .code("InternalError")
                        .build(),
                )
                .build()
        });

        let err = worker_delete(&[rule], &["prefix/dev/0", "prefix/dev/1"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("left 1 of 2 keys undeleted"), "{err}");
        assert!(err.contains("'prefix/dev/1': InternalError"), "{err}");
    }
}
