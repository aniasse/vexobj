use std::collections::HashSet;
use std::path::PathBuf;
use tracing::{info, warn};

use crate::db::Database;
use crate::error::StorageError;

pub struct GarbageCollector {
    data_dir: PathBuf,
}

#[derive(Debug, Default)]
pub struct GcResult {
    pub blobs_scanned: u64,
    pub orphans_removed: u64,
    pub bytes_freed: u64,
}

impl GarbageCollector {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Scan all blobs on disk and remove any that are not referenced in the database.
    pub fn collect(&self, db: &Database) -> Result<GcResult, StorageError> {
        let referenced = db.all_storage_paths()?;
        let referenced_set: HashSet<String> = referenced.into_iter().collect();

        let blobs_dir = self.data_dir.join("blobs");
        if !blobs_dir.exists() {
            return Ok(GcResult::default());
        }

        let mut result = GcResult::default();
        self.scan_dir(&blobs_dir, &referenced_set, &mut result)?;

        if result.orphans_removed > 0 {
            info!(
                orphans = result.orphans_removed,
                bytes_freed = result.bytes_freed,
                "garbage collection complete"
            );
        }

        Ok(result)
    }

    fn scan_dir(
        &self,
        dir: &std::path::Path,
        referenced: &HashSet<String>,
        result: &mut GcResult,
    ) -> Result<(), StorageError> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                self.scan_dir(&path, referenced, result)?;
                // Remove empty directories
                if std::fs::read_dir(&path)?.next().is_none() {
                    let _ = std::fs::remove_dir(&path);
                }
            } else {
                result.blobs_scanned += 1;

                // Compute relative path from data_dir
                let relative = path
                    .strip_prefix(&self.data_dir)
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();

                if !referenced.contains(&relative) {
                    let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    match std::fs::remove_file(&path) {
                        Ok(()) => {
                            result.orphans_removed += 1;
                            result.bytes_freed += size;
                        }
                        Err(e) => {
                            warn!(path = %path.display(), error = %e, "failed to remove orphan blob")
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
