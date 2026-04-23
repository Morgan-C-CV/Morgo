use std::path::Path;

use base64::Engine as _;
use image::imageops::FilterType;
use image::ImageReader;

use crate::core::message::ContentBlock;

const MAX_LONGEST_EDGE_PX: u32 = 1568;
const MAX_BYTES: usize = 5 * 1024 * 1024; // 5 MB

#[derive(Debug, thiserror::Error)]
pub enum AttachmentError {
    #[error("file not found: {0}")]
    NotFound(String),
    #[error("could not read file: {0}")]
    ReadError(String),
    #[error("unsupported file type (not an image): {0}")]
    UnsupportedType(String),
    #[error("image exceeds size limit after resize: {0}")]
    TooLarge(String),
    #[error("image decode failed: {0}")]
    DecodeFailed(String),
}

/// Supported MIME types for image attachments.
const SUPPORTED_MIME: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];

/// Load a local file path, detect its MIME type, resize if needed, and return
/// a ContentBlock::Image ready to be appended to a Message.
pub fn load_attachment(path: &str) -> Result<ContentBlock, AttachmentError> {
    let p = Path::new(path);
    if !p.exists() {
        return Err(AttachmentError::NotFound(path.to_string()));
    }

    let raw = std::fs::read(p)
        .map_err(|e| AttachmentError::ReadError(format!("{path}: {e}")))?;

    let kind = infer::get(&raw);
    let mime_type = kind
        .map(|k| k.mime_type())
        .unwrap_or("");

    if !SUPPORTED_MIME.contains(&mime_type) {
        return Err(AttachmentError::UnsupportedType(format!(
            "{path}: detected type '{mime_type}'"
        )));
    }

    let data = resize_if_needed(raw, mime_type, path)?;

    if data.len() > MAX_BYTES {
        return Err(AttachmentError::TooLarge(format!(
            "{path}: {} bytes after resize (limit {})",
            data.len(),
            MAX_BYTES
        )));
    }

    let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
    Ok(ContentBlock::Image {
        media_type: mime_type.to_string(),
        data: encoded.into_bytes(),
    })
}

fn resize_if_needed(raw: Vec<u8>, mime_type: &str, path: &str) -> Result<Vec<u8>, AttachmentError> {
    // If already within limits, skip decode/re-encode
    if raw.len() <= MAX_BYTES {
        let img = ImageReader::new(std::io::Cursor::new(&raw))
            .with_guessed_format()
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: {e}")))?
            .decode()
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: {e}")))?;

        let (w, h) = (img.width(), img.height());
        if w <= MAX_LONGEST_EDGE_PX && h <= MAX_LONGEST_EDGE_PX {
            return Ok(raw);
        }
        return encode_resized(img, mime_type, path);
    }

    // Oversized: must decode and resize
    let img = ImageReader::new(std::io::Cursor::new(&raw))
        .with_guessed_format()
        .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: {e}")))?
        .decode()
        .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: {e}")))?;

    encode_resized(img, mime_type, path)
}

fn encode_resized(
    img: image::DynamicImage,
    mime_type: &str,
    path: &str,
) -> Result<Vec<u8>, AttachmentError> {
    let (w, h) = (img.width(), img.height());
    let longest = w.max(h);
    let resized = if longest > MAX_LONGEST_EDGE_PX {
        let scale = MAX_LONGEST_EDGE_PX as f32 / longest as f32;
        let nw = (w as f32 * scale).round() as u32;
        let nh = (h as f32 * scale).round() as u32;
        img.resize(nw, nh, FilterType::Lanczos3)
    } else {
        img
    };

    let mut buf = Vec::new();
    match mime_type {
        "image/png" => resized
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: encode png: {e}")))?,
        "image/jpeg" => resized
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Jpeg)
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: encode jpeg: {e}")))?,
        "image/gif" => resized
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Gif)
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: encode gif: {e}")))?,
        "image/webp" => resized
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::WebP)
            .map_err(|e| AttachmentError::DecodeFailed(format!("{path}: encode webp: {e}")))?,
        _ => {
            return Err(AttachmentError::UnsupportedType(format!(
                "{path}: {mime_type}"
            )))
        }
    }
    Ok(buf)
}
