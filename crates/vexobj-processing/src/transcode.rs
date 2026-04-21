//! Video transcoding — wraps an ffmpeg subprocess with a small set of
//! preset profiles. Designed to be called from a worker task, not from
//! a request handler: transcodes take seconds to minutes.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct TranscodeProfile {
    pub name: &'static str,
    pub description: &'static str,
    /// Output file extension (no dot).
    pub extension: &'static str,
    /// MIME type the resulting object should be stored with.
    pub content_type: &'static str,
    /// ffmpeg command line arguments, inserted between `-i <input>`
    /// and the output path. Must NOT contain `-y` or the output path
    /// itself — the transcoder adds those.
    pub ffmpeg_args: &'static [&'static str],
    /// Wall-clock timeout. Protects against stalls on broken inputs.
    pub timeout_secs: u64,
}

/// Built-in profiles. Custom user profiles aren't supported in 0.1.x —
/// intentionally, because arbitrary ffmpeg args are a CVE waiting to
/// happen without careful sandboxing.
pub const PROFILES: &[TranscodeProfile] = &[
    TranscodeProfile {
        name: "webm-720p",
        description: "VP9 video + Opus audio, max 720p height, VBR 2 Mbps video",
        extension: "webm",
        content_type: "video/webm",
        ffmpeg_args: &[
            "-c:v", "libvpx-vp9",
            "-b:v", "2M",
            "-crf", "32",
            "-vf", "scale=-2:min(720\\,ih)",
            "-row-mt", "1",
            "-c:a", "libopus",
            "-b:a", "96k",
            "-deadline", "realtime",
            "-cpu-used", "5",
        ],
        timeout_secs: 600,
    },
    TranscodeProfile {
        name: "mp4-480p",
        description: "H.264 video + AAC audio, max 480p height, faststart for web",
        extension: "mp4",
        content_type: "video/mp4",
        ffmpeg_args: &[
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-vf", "scale=-2:min(480\\,ih)",
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
        ],
        timeout_secs: 600,
    },
    TranscodeProfile {
        name: "mp3-audio",
        description: "Audio-only MP3 192 kbps — useful for podcasts and voice tracks",
        extension: "mp3",
        content_type: "audio/mpeg",
        ffmpeg_args: &[
            "-vn",
            "-c:a", "libmp3lame",
            "-b:a", "192k",
        ],
        timeout_secs: 300,
    },
];

pub fn profile_by_name(name: &str) -> Option<&'static TranscodeProfile> {
    PROFILES.iter().find(|p| p.name == name)
}

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("unknown profile: {0}")]
    UnknownProfile(String),
    #[error("ffmpeg not installed on this host")]
    FfmpegMissing,
    #[error("ffmpeg timed out after {0}s")]
    Timeout(u64),
    #[error("ffmpeg failed with exit code {0}: {1}")]
    Failed(i32, String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Synchronous transcode: blocks until done. Callers should wrap this
/// in `tokio::task::spawn_blocking` or call it from a dedicated worker
/// thread. Writes the output to `dest` (any parent directories must
/// exist). Returns the output file size on success.
pub fn transcode(
    src: &Path,
    dest: &Path,
    profile: &TranscodeProfile,
) -> Result<u64, TranscodeError> {
    if !super::ffmpeg::probe_binary("ffmpeg") {
        return Err(TranscodeError::FfmpegMissing);
    }

    // Build the command: `ffmpeg -nostdin -loglevel error -y -i <src>
    // <profile args...> <dest>`. `-y` lets us overwrite partial output
    // from a previous failed attempt; if the output already existed
    // from a successful run the caller should have skipped us.
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-nostdin", "-loglevel", "error", "-y", "-i"])
        .arg(src)
        .args(profile.ffmpeg_args)
        .arg(dest);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stderr = child.stderr.take().expect("piped stderr");
    let (tx_err, rx_err) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut { stderr }, &mut buf);
        let _ = tx_err.send(buf);
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(profile.timeout_secs);
    let status = loop {
        if let Ok(Some(s)) = child.try_wait() { break s; }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(TranscodeError::Timeout(profile.timeout_secs));
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    let stderr_bytes = rx_err.recv().unwrap_or_default();
    if !status.success() {
        // ffmpeg's stderr is usually verbose; keep just the last line or two.
        let err = String::from_utf8_lossy(&stderr_bytes);
        let tail: String = err.lines().rev().take(3).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join(" | ");
        return Err(TranscodeError::Failed(status.code().unwrap_or(-1), tail));
    }

    let size = std::fs::metadata(dest)?.len();
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_are_unique_and_non_empty() {
        use std::collections::HashSet;
        assert!(!PROFILES.is_empty());
        let names: HashSet<_> = PROFILES.iter().map(|p| p.name).collect();
        assert_eq!(names.len(), PROFILES.len(), "duplicate profile names");
        for p in PROFILES {
            assert!(!p.extension.is_empty());
            assert!(!p.ffmpeg_args.is_empty());
            assert!(!p.description.is_empty());
            assert!(p.timeout_secs > 0);
        }
    }

    #[test]
    fn lookup_known_and_unknown() {
        assert!(profile_by_name("webm-720p").is_some());
        assert!(profile_by_name("mp3-audio").is_some());
        assert!(profile_by_name("does-not-exist").is_none());
    }
}
