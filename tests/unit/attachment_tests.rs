use std::fs;
use std::path::PathBuf;

use rust_agent::core::attachment::{AttachmentError, load_attachment};
use rust_agent::core::message::ContentBlock;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

#[test]
fn load_png_returns_image_block() {
    let path = fixture_path("test_image.png");
    let block = load_attachment(path.to_str().unwrap()).expect("png should load");
    match block {
        ContentBlock::Image { media_type, data } => {
            assert_eq!(media_type, "image/png");
            assert!(!data.is_empty());
        }
        other => panic!("expected Image block, got {:?}", other),
    }
}

#[test]
fn load_nonexistent_file_returns_not_found_error() {
    let err = load_attachment("/tmp/does_not_exist_12345.png").unwrap_err();
    assert!(matches!(err, AttachmentError::NotFound(_)));
}

#[test]
fn load_non_image_file_returns_unsupported_type_error() {
    let tmp = std::env::temp_dir().join("test_attachment_text.txt");
    fs::write(&tmp, b"hello world").unwrap();
    let err = load_attachment(tmp.to_str().unwrap()).unwrap_err();
    assert!(matches!(err, AttachmentError::UnsupportedType(_)));
    let _ = fs::remove_file(tmp);
}

#[test]
fn load_png_data_is_valid_base64() {
    let path = fixture_path("test_image.png");
    let block = load_attachment(path.to_str().unwrap()).expect("png should load");
    if let ContentBlock::Image { data, .. } = block {
        let s = String::from_utf8(data).expect("data should be utf-8 base64");
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .expect("data should be valid base64");
    }
}

#[test]
fn load_oversized_image_resizes_and_returns_image_block() {
    use image::{ImageBuffer, Rgb};
    // Create a 2000x2000 image (exceeds MAX_LONGEST_EDGE_PX=1568)
    let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::new(2000, 2000);
    let tmp = std::env::temp_dir().join("test_attachment_large.png");
    img.save(&tmp).expect("should save large png");
    let block = load_attachment(tmp.to_str().unwrap()).expect("large png should load after resize");
    assert!(matches!(block, ContentBlock::Image { .. }));
    let _ = std::fs::remove_file(tmp);
}
