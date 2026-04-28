use rust_agent::tool::builtin::bash::clamped_reader::{
    HEAD_BYTES, MAX_OUTPUT_BYTES, TAIL_BYTES, clamped_to_string, read_clamped,
};
use rust_agent::tool::builtin::bash::sandbox::{SandboxPolicy, execute_with_sandbox};

fn make_cursor(data: Vec<u8>) -> std::io::Cursor<Vec<u8>> {
    std::io::Cursor::new(data)
}

// ── read_clamped unit tests ──────────────────────────────────────────────────

#[tokio::test]
async fn output_within_limit_is_not_truncated() {
    let data = vec![b'X'; MAX_OUTPUT_BYTES / 2];
    let out = read_clamped(make_cursor(data.clone())).await;
    assert!(!out.truncated);
    assert_eq!(out.total_bytes_read, data.len());
    let s = clamped_to_string(out);
    assert!(!s.contains("[... output truncated:"), "unexpected truncation marker");
    assert_eq!(s.len(), data.len());
}

#[tokio::test]
async fn large_stdout_is_truncated_at_limit() {
    // 2 MiB — well over the 1 MiB limit
    let data = vec![b'A'; MAX_OUTPUT_BYTES * 2];
    let out = read_clamped(make_cursor(data)).await;
    assert!(out.truncated);
    assert!(out.total_bytes_read > MAX_OUTPUT_BYTES);
    let s = clamped_to_string(out);
    assert!(s.contains("[... output truncated:"), "truncation marker missing");
    // head preserved
    assert!(s.starts_with('A'));
}

#[tokio::test]
async fn truncation_shape_head_and_tail_preserved() {
    // head: HEAD_BYTES of 'H', filler: lots of 'M', tail: TAIL_BYTES of 'T'
    let mut data = vec![b'H'; HEAD_BYTES];
    data.extend(vec![b'M'; MAX_OUTPUT_BYTES]);
    data.extend(vec![b'T'; TAIL_BYTES]);

    let out = read_clamped(make_cursor(data)).await;
    assert!(out.truncated);

    // head bytes must all be 'H'
    assert!(out.head.iter().all(|&b| b == b'H'), "head corrupted");
    assert_eq!(out.head.len(), HEAD_BYTES);

    // tail bytes must all be 'T'
    assert!(out.tail.iter().all(|&b| b == b'T'), "tail corrupted");
    assert_eq!(out.tail.len(), TAIL_BYTES);

    let s = clamped_to_string(out);
    assert!(s.contains("[... output truncated:"));
    assert!(s.ends_with('T'));
}

#[tokio::test]
async fn empty_output_not_truncated() {
    let out = read_clamped(make_cursor(vec![])).await;
    assert!(!out.truncated);
    assert_eq!(out.total_bytes_read, 0);
    let s = clamped_to_string(out);
    assert!(s.is_empty());
}

// ── execute_with_sandbox integration tests ───────────────────────────────────

#[tokio::test]
async fn large_stdout_via_sandbox_is_clamped() {
    let cwd = std::env::temp_dir();
    // Generate ~2 MiB of output via dd
    let result = execute_with_sandbox(
        "dd if=/dev/zero bs=1024 count=2048 2>/dev/null | tr '\\0' 'A'",
        &cwd,
        SandboxPolicy::Disabled,
    )
    .await
    .expect("execute_with_sandbox failed");

    assert!(
        result.stdout.total_bytes_read > MAX_OUTPUT_BYTES,
        "expected > 1 MiB stdout, got {}",
        result.stdout.total_bytes_read
    );
    assert!(result.stdout.truncated, "stdout must be truncated");
    let s = clamped_to_string(result.stdout);
    assert!(s.contains("[... output truncated:"));
}

#[tokio::test]
async fn large_stderr_is_truncated_independently() {
    let cwd = std::env::temp_dir();
    // stdout is tiny; stderr is large
    let result = execute_with_sandbox(
        "dd if=/dev/zero bs=1024 count=2048 2>&1 1>/dev/null | tr '\\0' 'B' >&2; echo ok",
        &cwd,
        SandboxPolicy::Disabled,
    )
    .await
    .expect("execute_with_sandbox failed");

    // stdout should be small (just "ok\n")
    assert!(!result.stdout.truncated, "stdout should not be truncated");

    // stderr should be large and truncated
    if result.stderr.total_bytes_read > MAX_OUTPUT_BYTES {
        assert!(result.stderr.truncated);
        let s = clamped_to_string(result.stderr);
        assert!(s.contains("[... output truncated:"));
    }
    // If the shell redirected differently, at minimum stdout is not truncated
}

#[tokio::test]
async fn hostile_yes_command_does_not_oom() {
    let cwd = std::env::temp_dir();
    // yes produces infinite output; head -c limits it to 10 MiB at the shell level,
    // but our clamped reader must stop at MAX_OUTPUT_BYTES regardless.
    let result = execute_with_sandbox(
        "yes | head -c 10485760",
        &cwd,
        SandboxPolicy::Disabled,
    )
    .await
    .expect("execute_with_sandbox failed");

    // We must have read at least MAX_OUTPUT_BYTES and clamped it
    assert!(
        result.stdout.total_bytes_read >= MAX_OUTPUT_BYTES,
        "expected >= 1 MiB, got {}",
        result.stdout.total_bytes_read
    );
    assert!(result.stdout.truncated, "output must be truncated");
}
