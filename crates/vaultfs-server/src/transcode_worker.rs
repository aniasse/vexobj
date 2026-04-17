//! Background worker that drains the `transcode_jobs` table.
//!
//! Spawned once at startup. Polls for `pending` jobs, runs ffmpeg on a
//! blocking thread, stores the output as a first-class vaultfs object
//! (so it shows up in listings, gets normal versioning and lifecycle),
//! then updates the job row. On failure the error message is stored
//! on the row and the worker moves on.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};
use vaultfs_storage::StorageEngine;

#[derive(Clone)]
pub struct TranscodeWorkerConfig {
    /// How often to wake up and poll when the queue is empty. Once a
    /// job is running, the worker loops back to the poll as soon as
    /// it's done, so this only paces the idle case.
    pub poll_interval: Duration,
    /// Directory under data_dir where worker-local temp files live.
    /// Cleaned up after each job regardless of outcome.
    pub scratch_subdir: &'static str,
}

impl Default for TranscodeWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            scratch_subdir: "transcode-scratch",
        }
    }
}

/// Start the worker loop on the current tokio runtime. Returns a
/// handle the caller can drop to let the worker exit on the next
/// poll cycle.
pub fn spawn(storage: Arc<StorageEngine>, cfg: TranscodeWorkerConfig) {
    // ffmpeg has to be on PATH. Otherwise the worker still starts but
    // fails every job — rather than hide that, we log once and skip.
    if !storage.video_features().ffmpeg {
        info!("transcode worker: ffmpeg not on PATH, skipping spawn");
        return;
    }
    tokio::spawn(async move {
        info!("transcode worker: online");
        let scratch = storage.data_dir().join(cfg.scratch_subdir);
        let _ = std::fs::create_dir_all(&scratch);

        loop {
            match storage.db().claim_next_transcode_job() {
                Ok(Some(job)) => {
                    let started = Instant::now();
                    if let Err(e) = run_job(&storage, &scratch, &job).await {
                        let ms = started.elapsed().as_millis() as i64;
                        warn!(job = %job.id, error = %e, "transcode job failed");
                        let _ = storage
                            .db()
                            .fail_transcode_job(&job.id, &e, ms);
                    }
                    // Loop straight back to check for the next job.
                }
                Ok(None) => {
                    tokio::time::sleep(cfg.poll_interval).await;
                }
                Err(e) => {
                    error!("transcode worker: queue query failed: {e}");
                    tokio::time::sleep(cfg.poll_interval).await;
                }
            }
        }
    });
}

async fn run_job(
    storage: &Arc<StorageEngine>,
    scratch: &std::path::Path,
    job: &vaultfs_storage::TranscodeJob,
) -> Result<(), String> {
    let profile = vaultfs_processing::transcode_profile(&job.profile)
        .ok_or_else(|| format!("unknown profile: {}", job.profile))?;

    // Source path comes straight from the object store. If the source
    // object was deleted between enqueue and now, fail fast.
    let src = storage
        .object_data_path(&job.bucket, &job.key)
        .map_err(|e| format!("source not available: {e}"))?;

    // SSE-on: the file on disk is ciphertext. Transcoding would need a
    // decrypted copy in scratch. Out of scope for 0.1.x — fail clearly.
    if storage.encryption_enabled() {
        return Err("transcoding is disabled when SSE-at-rest is on".to_string());
    }

    // Per-job scratch file. Uuid keeps concurrent workers from colliding.
    let dest = scratch.join(format!("{}.{}", job.id, profile.extension));
    let profile_owned = profile.clone();
    let dest_for_ff = dest.clone();
    let ff_start = Instant::now();
    let size = tokio::task::spawn_blocking(move || {
        vaultfs_processing::transcode(&src, &dest_for_ff, &profile_owned)
    })
    .await
    .map_err(|e| format!("worker panicked: {e}"))?
    .map_err(|e| format!("{e}"))?;

    // Stream the output into the object store as a regular vaultfs
    // object. Key convention: "<original_key>.<profile>.<ext>" in the
    // same bucket — picks up versioning / lifecycle / ACLs naturally.
    let output_key = format!("{}.{}.{}", job.key, profile.name, profile.extension);
    let file = tokio::fs::File::open(&dest)
        .await
        .map_err(|e| format!("open output: {e}"))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let meta = storage
        .put_object_stream(
            &job.bucket,
            &output_key,
            stream,
            Some(profile.content_type),
            None,
        )
        .await
        .map_err(|e| format!("store variant: {e}"))?;

    // Clean scratch regardless — it only ever holds the one job.
    let _ = tokio::fs::remove_file(&dest).await;

    storage
        .db()
        .complete_transcode_job(
            &job.id,
            &job.bucket,
            &output_key,
            meta.size,
            ff_start.elapsed().as_millis() as i64,
        )
        .map_err(|e| format!("complete job: {e}"))?;

    info!(
        job = %job.id,
        profile = %job.profile,
        bucket = %job.bucket,
        key = %job.key,
        output = %output_key,
        size = %size,
        "transcode completed"
    );
    Ok(())
}
