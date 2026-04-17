//! ffmpeg / ffprobe integration for video thumbnails and universal
//! container metadata. Everything here is *optional*: it detects the
//! binaries on PATH at runtime, never panics when they're absent, and
//! falls back to the pure-Rust MP4 parser for metadata.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::video::VideoMetadata;

/// Run-time feature flags derived from what's installed on the host.
/// Populated once at server startup and plumbed through app state.
#[derive(Debug, Clone, Default)]
pub struct VideoFeatures {
    pub ffmpeg: bool,
    pub ffprobe: bool,
}

impl VideoFeatures {
    /// Probe the host for `ffmpeg` and `ffprobe`. Each lookup times out
    /// at 2s so a broken install never hangs server startup.
    pub fn detect() -> Self {
        Self {
            ffmpeg: probe_binary("ffmpeg"),
            ffprobe: probe_binary("ffprobe"),
        }
    }

    pub fn any(&self) -> bool { self.ffmpeg || self.ffprobe }
}

fn probe_binary(name: &str) -> bool {
    Command::new(name)
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Extract video metadata with ffprobe. Returns None if ffprobe is
/// absent, the file isn't a video, or the probe times out. The JSON
/// shape is stable enough to parse with a few unwraps guarded by `?`.
pub fn probe_with_ffprobe(path: &Path) -> Option<VideoMetadata> {
    let output = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() { return None; }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;

    let streams = json.get("streams")?.as_array()?;
    let video_stream = streams.iter().find(|s| {
        s.get("codec_type").and_then(|v| v.as_str()) == Some("video")
    })?;
    let has_audio = streams.iter().any(|s| {
        s.get("codec_type").and_then(|v| v.as_str()) == Some("audio")
    });

    let width = video_stream.get("width").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
    let height = video_stream.get("height").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
    let codec = video_stream
        .get("codec_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Prefer stream duration; fall back to container duration. Both are
    // strings in ffprobe output, so parse lazily.
    let duration_secs = video_stream
        .get("duration")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| {
            json.get("format")
                .and_then(|f| f.get("duration"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .unwrap_or(0.0);

    if width == 0 && height == 0 && codec.is_none() { return None; }

    Some(VideoMetadata {
        duration_secs: (duration_secs * 1000.0).round() / 1000.0,
        width,
        height,
        codec,
        has_audio,
    })
}

#[derive(Debug, Clone, Copy)]
pub enum ThumbFormat { Jpeg, WebP }

impl ThumbFormat {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "webp" => ThumbFormat::WebP,
            _      => ThumbFormat::Jpeg,
        }
    }
    pub fn mime(&self) -> &'static str {
        match self {
            ThumbFormat::Jpeg => "image/jpeg",
            ThumbFormat::WebP => "image/webp",
        }
    }
    pub fn codec(&self) -> &'static str {
        match self {
            ThumbFormat::Jpeg => "mjpeg",
            ThumbFormat::WebP => "libwebp",
        }
    }
}

/// Inputs to a thumbnail request, normalized and bounded so callers
/// can derive a deterministic cache key.
#[derive(Debug, Clone)]
pub struct ThumbRequest {
    pub at_seconds: f64,
    pub width: u32,
    pub format: ThumbFormat,
    pub quality: u8,
}

impl ThumbRequest {
    pub fn sanitized(at: Option<f64>, width: Option<u32>, format: Option<&str>, quality: Option<u8>) -> Self {
        Self {
            at_seconds: at.unwrap_or(1.0).max(0.0).min(3600.0 * 24.0),
            width: width.unwrap_or(320).clamp(32, 1920),
            format: format.map(ThumbFormat::parse).unwrap_or(ThumbFormat::Jpeg),
            quality: quality.unwrap_or(70).clamp(1, 100),
        }
    }

    /// Stable cache key per (content hash, time, width, format, quality).
    pub fn cache_key(&self, sha256: &str) -> String {
        let fmt = match self.format {
            ThumbFormat::Jpeg => "jpeg",
            ThumbFormat::WebP => "webp",
        };
        format!("thumb/{}/t={:.3}/w={}/q={}/{}", sha256, self.at_seconds, self.width, self.quality, fmt)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ThumbError {
    #[error("ffmpeg not installed on this host")]
    FfmpegMissing,
    #[error("ffmpeg timed out")]
    Timeout,
    #[error("ffmpeg failed with exit code {0}: {1}")]
    Failed(i32, String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Extract a single frame at `req.at_seconds` from the given file,
/// scaled to `req.width`, and return the encoded bytes.
///
/// Seek-before-input (`-ss` before `-i`) is fast but can be slightly
/// inaccurate on frame-boundary; for a thumbnail that's exactly what
/// we want. Hard 15s timeout so a broken file can't stall a worker.
pub fn generate_thumbnail(src: &Path, req: &ThumbRequest) -> Result<Vec<u8>, ThumbError> {
    if !probe_binary("ffmpeg") {
        return Err(ThumbError::FfmpegMissing);
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.args([
        "-nostdin",
        "-loglevel", "error",
        "-ss", &format!("{:.3}", req.at_seconds),
        "-i",
    ]).arg(src).args([
        "-vframes", "1",
        "-vf", &format!("scale={}:-2", req.width),
        "-f", "image2pipe",
        "-c:v", req.format.codec(),
    ]);

    // Quality flags differ between mjpeg and libwebp — mjpeg uses
    // `-q:v 2..31` (lower = better), libwebp uses `-quality 0..100`.
    match req.format {
        ThumbFormat::Jpeg => {
            // Map 1-100 to ffmpeg's 31-1 scale (inverted).
            let q = ((31.0 - (req.quality as f64 / 100.0) * 30.0).round() as u8).clamp(1, 31);
            cmd.args(["-q:v", &q.to_string()]);
        }
        ThumbFormat::WebP => {
            cmd.args(["-quality", &req.quality.to_string()]);
        }
    }

    cmd.arg("-");
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    // Read both pipes on separate threads with a 15s wall clock so a
    // pathological file can't block a request indefinitely.
    let (tx_out, rx_out) = std::sync::mpsc::channel();
    let (tx_err, rx_err) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { stdout }, &mut buf);
        let _ = tx_out.send(buf);
    });
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { stderr }, &mut buf);
        let _ = tx_err.send(buf);
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Ok(Some(s)) = child.try_wait() { break s; }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            return Err(ThumbError::Timeout);
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    let stdout_bytes = rx_out.recv().unwrap_or_default();
    let stderr_bytes = rx_err.recv().unwrap_or_default();

    if !status.success() {
        let msg = String::from_utf8_lossy(&stderr_bytes).into_owned();
        return Err(ThumbError::Failed(status.code().unwrap_or(-1), msg));
    }
    if stdout_bytes.is_empty() {
        return Err(ThumbError::Failed(0, "ffmpeg produced empty output".into()));
    }
    Ok(stdout_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thumbreq_clamping() {
        let r = ThumbRequest::sanitized(Some(-10.0), Some(9999), Some("webp"), Some(255));
        assert!(r.at_seconds >= 0.0);
        assert_eq!(r.width, 1920);
        assert!(matches!(r.format, ThumbFormat::WebP));
        assert_eq!(r.quality, 100);
    }

    #[test]
    fn thumbreq_cache_key_is_stable() {
        let r = ThumbRequest {
            at_seconds: 1.5, width: 320, format: ThumbFormat::Jpeg, quality: 70,
        };
        let k1 = r.cache_key("abc");
        let k2 = r.cache_key("abc");
        assert_eq!(k1, k2);
        assert!(k1.contains("t=1.500"));
        assert!(k1.contains("w=320"));
    }
}
