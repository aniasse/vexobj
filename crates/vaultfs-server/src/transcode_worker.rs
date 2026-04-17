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
    /// Number of worker tasks to spawn. Each polls the queue
    /// independently; `claim_next_transcode_job` is an atomic
    /// transaction so they never take the same job.
    pub workers: u32,
    /// How often to wake up and poll when the queue is empty. Once a
    /// job is running, the worker loops back to the poll as soon as
    /// it's done, so this only paces the idle case.
    pub poll_interval: Duration,
    /// Directory under data_dir where worker-local temp files live.
    /// Cleaned up after each job regardless of outcome.
    pub scratch_subdir: &'static str,
    /// Terminal jobs (`completed` / `failed`) older than this get GC'd
    /// on an hourly sweep. 0 disables GC entirely.
    pub gc_after_days: u32,
}

impl Default for TranscodeWorkerConfig {
    fn default() -> Self {
        Self {
            workers: 2,
            poll_interval: Duration::from_secs(2),
            scratch_subdir: "transcode-scratch",
            gc_after_days: 30,
        }
    }
}

/// Start the worker pool + periodic GC on the current tokio runtime.
pub fn spawn(storage: Arc<StorageEngine>, cfg: TranscodeWorkerConfig) {
    // ffmpeg has to be on PATH. Otherwise the worker still starts but
    // fails every job — rather than hide that, we log once and skip.
    if !storage.video_features().ffmpeg {
        info!("transcode worker: ffmpeg not on PATH, skipping spawn");
        return;
    }

    let scratch = storage.data_dir().join(cfg.scratch_subdir);
    let _ = std::fs::create_dir_all(&scratch);

    // Spawn N independent worker loops. They share the queue via the
    // atomic claim method; at most one of them will take any given job.
    let worker_count = cfg.workers.max(1);
    info!(workers = worker_count, "transcode worker pool online");
    for worker_id in 0..worker_count {
        let storage = storage.clone();
        let scratch = scratch.clone();
        let poll = cfg.poll_interval;
        tokio::spawn(async move {
            loop {
                match storage.db().claim_next_transcode_job() {
                    Ok(Some(job)) => {
                        let started = Instant::now();
                        if let Err(e) = run_job(&storage, &scratch, &job).await {
                            let ms = started.elapsed().as_millis() as i64;
                            warn!(worker = worker_id, job = %job.id, error = %e, "transcode job failed");
                            let _ = storage.db().fail_transcode_job(&job.id, &e, ms);
                        }
                    }
                    Ok(None) => tokio::time::sleep(poll).await,
                    Err(e) => {
                        error!(worker = worker_id, "transcode queue query failed: {e}");
                        tokio::time::sleep(poll).await;
                    }
                }
            }
        });
    }

    // Periodic GC: removes terminal rows older than the configured
    // retention. Runs hourly; one instance is enough regardless of
    // worker count.
    if cfg.gc_after_days > 0 {
        let storage = storage.clone();
        let retention_days = cfg.gc_after_days as i64;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days);
                match storage.db().gc_transcode_jobs(cutoff) {
                    Ok(n) if n > 0 => info!(removed = n, "transcode queue GC"),
                    Ok(_) => {}
                    Err(e) => warn!("transcode GC failed: {e}"),
                }
            }
        });
    }
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
