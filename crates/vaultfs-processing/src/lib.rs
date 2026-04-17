mod error;
mod transform;
mod video;

pub use error::ProcessingError;
pub use transform::*;
pub use video::{
    is_probable as is_probable_video,
    probe_bytes as probe_video_bytes,
    probe_file as probe_video_file,
    VideoMetadata,
};
