//! Video metadata extraction for MP4/MOV containers.
//!
//! Uses the pure-Rust `mp4` crate so we keep the "single binary, no
//! external runtime" promise — no ffmpeg dependency. Only *metadata*
//! is extracted; thumbnail generation and transcoding need a decoder
//! and are deliberately out of scope for 0.1.x.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Subset of video container metadata that's cheap to extract and
/// worth surfacing to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMetadata {
    /// Total duration in seconds. Rounded to millisecond precision.
    pub duration_secs: f64,
    pub width: u16,
    pub height: u16,
    /// Codec short name of the first video track, e.g. "h264", "h265",
    /// "vp9", "av1". None when the container has no video track (audio
    /// only) or the codec is unknown.
    pub codec: Option<String>,
    /// True when the container has at least one audio track.
    pub has_audio: bool,
}

/// Heuristic gate: caller tells us the declared content-type; we
/// return true if we'd try to probe it. Containers we can't parse
/// (webm, mkv, avi…) fall through without error.
pub fn is_probable(content_type: &str) -> bool {
    matches!(
        content_type,
        "video/mp4" | "video/quicktime" | "video/x-m4v" | "video/mpeg" | "video/3gpp"
    )
}

/// Probe an on-disk file. Returns None when the file isn't a parseable
/// MP4/MOV — any I/O or container error is treated as "no metadata"
/// rather than an upload failure, since metadata is best-effort.
pub fn probe_file(path: &Path) -> Option<VideoMetadata> {
    let file = std::fs::File::open(path).ok()?;
    let size = file.metadata().ok()?.len();
    let reader = std::io::BufReader::new(file);
    probe_mp4(reader, size)
}

/// Same as `probe_file` but for an in-memory buffer — used by the
/// engine when the on-disk copy is encrypted and unparseable as-is.
pub fn probe_bytes(bytes: &[u8]) -> Option<VideoMetadata> {
    let size = bytes.len() as u64;
    let reader = std::io::Cursor::new(bytes);
    probe_mp4(reader, size)
}

fn probe_mp4<R: std::io::Read + std::io::Seek>(reader: R, size: u64) -> Option<VideoMetadata> {
    let mp4 = mp4::Mp4Reader::read_header(reader, size).ok()?;

    let duration_secs = mp4.duration().as_secs_f64();
    let mut width: u16 = 0;
    let mut height: u16 = 0;
    let mut codec: Option<String> = None;
    let mut has_audio = false;

    for (_id, track) in mp4.tracks() {
        use mp4::TrackType;
        match track.track_type().ok()? {
            TrackType::Video => {
                if width == 0 {
                    width = track.width();
                    height = track.height();
                }
                if codec.is_none() {
                    codec = track.media_type().ok().map(|m| m.to_string());
                }
            }
            TrackType::Audio => {
                has_audio = true;
            }
            _ => {}
        }
    }

    // A container with zero video tracks isn't a video from our
    // perspective, even if the MIME claims otherwise.
    if width == 0 && height == 0 && codec.is_none() {
        return None;
    }

    Some(VideoMetadata {
        duration_secs: (duration_secs * 1000.0).round() / 1000.0,
        width,
        height,
        codec,
        has_audio,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_probable_accepts_common_mp4_mimes() {
        assert!(is_probable("video/mp4"));
        assert!(is_probable("video/quicktime"));
        assert!(is_probable("video/x-m4v"));
        assert!(!is_probable("video/webm"));
        assert!(!is_probable("image/jpeg"));
    }

    #[test]
    fn probe_file_returns_none_for_missing_or_broken() {
        assert!(probe_file(Path::new("/nonexistent/path/to/nothing.mp4")).is_none());

        // Write garbage to a temp file — mp4::read_header should
        // bail, and we should return None rather than panic.
        let tmp = std::env::temp_dir().join("vfs-broken-mp4.bin");
        std::fs::write(&tmp, b"this is not an mp4 container at all").unwrap();
        assert!(probe_file(&tmp).is_none());
        let _ = std::fs::remove_file(&tmp);
    }
}
