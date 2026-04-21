use image::imageops::FilterType;
use image::{DynamicImage, ImageFormat, ImageReader};
use std::io::Cursor;

use crate::error::ProcessingError;

#[derive(Debug, Clone, Default)]
pub struct TransformParams {
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub format: Option<OutputFormat>,
    pub quality: Option<u8>,
    pub fit: FitMode,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum FitMode {
    #[default]
    Cover,
    Contain,
    Fill,
}

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Jpeg,
    Png,
    WebP,
    Avif,
    Gif,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "jpg" | "jpeg" => Some(Self::Jpeg),
            "png" => Some(Self::Png),
            "webp" => Some(Self::WebP),
            "avif" => Some(Self::Avif),
            "gif" => Some(Self::Gif),
            _ => None,
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::WebP => "image/webp",
            Self::Avif => "image/avif",
            Self::Gif => "image/gif",
        }
    }

    pub fn image_format(&self) -> ImageFormat {
        match self {
            Self::Jpeg => ImageFormat::Jpeg,
            Self::Png => ImageFormat::Png,
            Self::WebP => ImageFormat::WebP,
            Self::Avif => ImageFormat::Avif,
            Self::Gif => ImageFormat::Gif,
        }
    }
}

/// Detect the best output format from the Accept header
pub fn best_format_from_accept(accept: &str) -> Option<OutputFormat> {
    // Prefer AVIF > WebP if both are accepted
    if accept.contains("image/avif") {
        Some(OutputFormat::Avif)
    } else if accept.contains("image/webp") {
        Some(OutputFormat::WebP)
    } else {
        None
    }
}

pub fn transform_image(
    data: &[u8],
    params: &TransformParams,
) -> Result<(Vec<u8>, String), ProcessingError> {
    let img = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| ProcessingError::Decode(image::ImageError::IoError(e)))?
        .decode()?;

    let img = apply_resize(&img, params);

    let format = params.format.unwrap_or(OutputFormat::Jpeg);
    let quality = params.quality.unwrap_or(85);

    let mut output = Vec::new();
    let mut cursor = Cursor::new(&mut output);

    match format {
        OutputFormat::Jpeg => {
            let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut cursor, quality);
            img.write_with_encoder(encoder)?;
        }
        OutputFormat::Png => {
            img.write_to(&mut cursor, ImageFormat::Png)?;
        }
        OutputFormat::WebP => {
            img.write_to(&mut cursor, ImageFormat::WebP)?;
        }
        OutputFormat::Avif => {
            img.write_to(&mut cursor, ImageFormat::Avif)?;
        }
        OutputFormat::Gif => {
            img.write_to(&mut cursor, ImageFormat::Gif)?;
        }
    }

    Ok((output, format.content_type().to_string()))
}

fn apply_resize(img: &DynamicImage, params: &TransformParams) -> DynamicImage {
    match (params.width, params.height) {
        (Some(w), Some(h)) => match params.fit {
            FitMode::Cover => img.resize_to_fill(w, h, FilterType::Lanczos3),
            FitMode::Contain => img.resize(w, h, FilterType::Lanczos3),
            FitMode::Fill => img.resize_exact(w, h, FilterType::Lanczos3),
        },
        (Some(w), None) => {
            let ratio = w as f64 / img.width() as f64;
            let h = (img.height() as f64 * ratio) as u32;
            img.resize_exact(w, h, FilterType::Lanczos3)
        }
        (None, Some(h)) => {
            let ratio = h as f64 / img.height() as f64;
            let w = (img.width() as f64 * ratio) as u32;
            img.resize_exact(w, h, FilterType::Lanczos3)
        }
        (None, None) => img.clone(),
    }
}

impl TransformParams {
    pub fn has_transforms(&self) -> bool {
        self.width.is_some() || self.height.is_some() || self.format.is_some() || self.quality.is_some()
    }

    pub fn cache_key(&self) -> String {
        format!(
            "w={}&h={}&f={}&q={}&fit={}",
            self.width.map(|v| v.to_string()).unwrap_or_default(),
            self.height.map(|v| v.to_string()).unwrap_or_default(),
            self.format
                .map(|f| f.content_type().to_string())
                .unwrap_or_default(),
            self.quality.unwrap_or(85),
            match self.fit {
                FitMode::Cover => "cover",
                FitMode::Contain => "contain",
                FitMode::Fill => "fill",
            }
        )
    }
}
