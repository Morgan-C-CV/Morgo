use std::collections::VecDeque;

use tokio::io::AsyncReadExt;

pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024; // 1 MiB
pub const HEAD_BYTES: usize = 32 * 1024; // 32 KiB
pub const TAIL_BYTES: usize = 32 * 1024; // 32 KiB

const CHUNK: usize = 8 * 1024; // 8 KiB read chunk

pub struct ClampedOutput {
    pub head: Vec<u8>,
    pub tail: Vec<u8>,
    pub truncated: bool,
    pub total_bytes_read: usize,
}

/// Read from `reader`, keeping at most HEAD_BYTES from the start and TAIL_BYTES from the end.
/// If total bytes read exceeds MAX_OUTPUT_BYTES the output is marked truncated.
/// If total bytes read is within the limit, the full output is returned (head contains all, tail is empty).
pub async fn read_clamped<R: AsyncReadExt + Unpin>(mut reader: R) -> ClampedOutput {
    // Phase 1: accumulate up to MAX_OUTPUT_BYTES into a single buffer.
    let mut full: Vec<u8> = Vec::with_capacity(MAX_OUTPUT_BYTES.min(64 * 1024));
    let mut buf = vec![0u8; CHUNK];
    let mut total: usize = 0;
    let mut over_limit = false;

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        total += n;

        if !over_limit && full.len() + n <= MAX_OUTPUT_BYTES {
            full.extend_from_slice(&buf[..n]);
        } else {
            // We've hit or exceeded the limit — switch to head/tail mode.
            over_limit = true;
            // full already has up to MAX_OUTPUT_BYTES; we don't need it anymore.
            // Break out and re-read from scratch using head/tail strategy.
            // But we can't seek back, so we need to handle this inline.
            // Instead: drain `full` into head/tail, then continue reading.
            break;
        }
    }

    if !over_limit {
        // All data fit within the limit.
        return ClampedOutput {
            head: full,
            tail: vec![],
            truncated: false,
            total_bytes_read: total,
        };
    }

    // We exceeded the limit. Rebuild head/tail from what we have in `full` plus remaining stream.
    // `full` contains up to MAX_OUTPUT_BYTES bytes already read.
    let mut head: Vec<u8> = Vec::with_capacity(HEAD_BYTES);
    let mut tail: VecDeque<u8> = VecDeque::with_capacity(TAIL_BYTES + CHUNK);

    // Feed `full` into head/tail.
    feed_into_head_tail(&mut head, &mut tail, &full);
    drop(full);

    // Continue reading the rest of the stream.
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        total += n;
        feed_into_head_tail(&mut head, &mut tail, &buf[..n]);
    }

    let tail_vec: Vec<u8> = tail.into_iter().collect();
    ClampedOutput {
        head,
        tail: tail_vec,
        truncated: true,
        total_bytes_read: total,
    }
}

fn feed_into_head_tail(head: &mut Vec<u8>, tail: &mut VecDeque<u8>, data: &[u8]) {
    for &b in data {
        if head.len() < HEAD_BYTES {
            head.push(b);
        } else {
            if tail.len() == TAIL_BYTES {
                tail.pop_front();
            }
            tail.push_back(b);
        }
    }
}

/// Convert a `ClampedOutput` to a lossy UTF-8 string.
/// If truncated, inserts a marker between head and tail.
pub fn clamped_to_string(output: ClampedOutput) -> String {
    if !output.truncated {
        let mut all = output.head;
        all.extend_from_slice(&output.tail);
        return String::from_utf8_lossy(&all).into_owned();
    }

    let head_str = String::from_utf8_lossy(&output.head);
    let tail_str = String::from_utf8_lossy(&output.tail);
    let marker = format!(
        "\n[... output truncated: {} bytes read, showing first {}B and last {}B ...]\n",
        output.total_bytes_read, HEAD_BYTES, TAIL_BYTES,
    );
    format!("{}{}{}", head_str, marker, tail_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_reader(data: Vec<u8>) -> std::io::Cursor<Vec<u8>> {
        std::io::Cursor::new(data)
    }

    #[tokio::test]
    async fn small_output_not_truncated() {
        let data = b"hello world".to_vec();
        let out = read_clamped(make_reader(data.clone())).await;
        assert!(!out.truncated);
        assert_eq!(out.total_bytes_read, data.len());
        let s = clamped_to_string(out);
        assert_eq!(s, "hello world");
    }

    #[tokio::test]
    async fn large_output_truncated_with_marker() {
        let data = vec![b'A'; MAX_OUTPUT_BYTES + 1];
        let out = read_clamped(make_reader(data)).await;
        assert!(out.truncated);
        let s = clamped_to_string(out);
        assert!(s.contains("[... output truncated:"), "marker missing");
        assert!(s.starts_with('A'), "head not preserved");
    }

    #[tokio::test]
    async fn head_and_tail_boundaries_correct() {
        let mut data = vec![b'H'; HEAD_BYTES];
        data.extend(vec![b'M'; MAX_OUTPUT_BYTES]);
        data.extend(vec![b'T'; TAIL_BYTES]);
        let out = read_clamped(make_reader(data)).await;
        assert!(out.truncated);
        assert!(out.head.iter().all(|&b| b == b'H'));
        assert!(out.tail.iter().all(|&b| b == b'T'));
    }

    #[tokio::test]
    async fn within_limit_full_content_preserved() {
        let data = vec![b'X'; MAX_OUTPUT_BYTES / 2];
        let out = read_clamped(make_reader(data.clone())).await;
        assert!(!out.truncated);
        assert_eq!(out.total_bytes_read, data.len());
        let s = clamped_to_string(out);
        assert_eq!(s.len(), data.len());
    }
}
