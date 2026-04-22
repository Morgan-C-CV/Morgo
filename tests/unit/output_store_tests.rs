use rust_agent::task::output_store::TaskOutputStore;

fn store_with_file(content: &str) -> (TaskOutputStore, String) {
    let store = TaskOutputStore::default();
    let task_id = format!(
        "test-output-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let path = store.init(&task_id).unwrap();
    store.append(&path, content).unwrap();
    (store, path)
}

#[test]
fn read_slice_returns_full_content_for_small_output() {
    let payload = "hello world\nline two\n";
    let (store, path) = store_with_file(payload);
    let slice = store.read_slice(&path, 0).unwrap();
    assert_eq!(slice.content, payload);
    assert_eq!(slice.next_offset, payload.len());
    assert!(!slice.content.contains("[truncated"));
}

#[test]
fn read_slice_returns_tail_with_truncated_marker_when_over_cap() {
    // Write 300 KB — well over the 256 KB cap.
    let chunk = "x".repeat(1024);
    let (store, path) = {
        let s = TaskOutputStore::default();
        let task_id = format!(
            "test-large-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let p = s.init(&task_id).unwrap();
        for _ in 0..300 {
            s.append(&p, &chunk).unwrap();
        }
        (s, p)
    };

    let slice = store.read_slice(&path, 0).unwrap();
    // Content must start with the truncated marker.
    assert!(
        slice.content.starts_with("[truncated:"),
        "expected truncated marker, got: {}",
        &slice.content[..80.min(slice.content.len())]
    );
    // next_offset must reflect the true file size (300 KB).
    assert_eq!(slice.next_offset, 300 * 1024);
    // The returned content (marker + tail) must not exceed cap + marker overhead.
    // Marker is small; tail is at most 256 KB.
    assert!(slice.content.len() <= 256 * 1024 + 128);
}

#[test]
fn read_slice_offset_at_end_returns_empty() {
    let payload = "some output";
    let (store, path) = store_with_file(payload);
    let slice = store.read_slice(&path, payload.len()).unwrap();
    assert_eq!(slice.content, "");
    assert_eq!(slice.next_offset, payload.len());
}

#[test]
fn read_slice_offset_beyond_cap_window_returns_truncated_marker() {
    // Write 300 KB, then request from offset 0 — which is before the 256 KB cap window.
    let chunk = "y".repeat(1024);
    let store = TaskOutputStore::default();
    let task_id = format!(
        "test-offset-cap-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let path = store.init(&task_id).unwrap();
    for _ in 0..300 {
        store.append(&path, &chunk).unwrap();
    }

    // offset=0 is before cap_start (300KB - 256KB = 44KB), so we get the truncated marker.
    let slice = store.read_slice(&path, 0).unwrap();
    assert!(slice.content.starts_with("[truncated:"));
    // next_offset is always the true file end.
    assert_eq!(slice.next_offset, 300 * 1024);
}
