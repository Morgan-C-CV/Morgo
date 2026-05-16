use rust_agent::command::types::CommandResult;
use rust_agent::core::output::{OutputBlock, blocks_to_plain_text};

// --- OutputBlock::to_plain_text correctness ---

#[test]
fn text_block_renders_as_is() {
    let block = OutputBlock::text("hello world");
    assert_eq!(block.to_plain_text(), "hello world");
}

#[test]
fn kv_block_renders_with_dash_prefix() {
    let block = OutputBlock::kv("session_id", "abc-123");
    assert_eq!(block.to_plain_text(), "- session_id: abc-123");
}

#[test]
fn section_block_indents_children() {
    let block = OutputBlock::section(
        "Runtime",
        vec![
            OutputBlock::kv("session_id", "abc"),
            OutputBlock::kv("surface", "Cli"),
        ],
    );
    let text = block.to_plain_text();
    assert!(
        text.starts_with("Runtime:"),
        "should start with section title"
    );
    assert!(
        text.contains("  - session_id: abc"),
        "child should be indented"
    );
    assert!(
        text.contains("  - surface: Cli"),
        "child should be indented"
    );
}

#[test]
fn nested_section_double_indents() {
    let inner = OutputBlock::section("inner", vec![OutputBlock::kv("k", "v")]);
    let outer = OutputBlock::section("outer", vec![inner]);
    let text = outer.to_plain_text();
    assert!(
        text.contains("    - k: v"),
        "nested section should double-indent kv"
    );
}

#[test]
fn table_renders_aligned_columns() {
    let block = OutputBlock::table(
        vec!["Name".into(), "Count".into()],
        vec![
            vec!["alpha".into(), "1".into()],
            vec!["beta".into(), "20".into()],
        ],
    );
    let text = block.to_plain_text();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 4, "header + sep + 2 rows");
    assert!(lines[0].starts_with("Name"), "header first");
    assert!(lines[1].starts_with("-----"), "separator second");
    assert!(lines[2].starts_with("alpha"), "first data row");
    assert!(lines[3].starts_with("beta"), "second data row");
}

#[test]
fn empty_table_renders_empty_string() {
    let block = OutputBlock::table(vec![], vec![]);
    assert_eq!(block.to_plain_text(), "");
}

// --- blocks_to_plain_text joins non-empty blocks ---

#[test]
fn blocks_to_plain_text_joins_with_newline() {
    let blocks = vec![OutputBlock::text("line one"), OutputBlock::text("line two")];
    assert_eq!(blocks_to_plain_text(&blocks), "line one\nline two");
}

#[test]
fn blocks_to_plain_text_skips_empty_blocks() {
    let blocks = vec![
        OutputBlock::text("first"),
        OutputBlock::table(vec![], vec![]),
        OutputBlock::text("last"),
    ];
    assert_eq!(blocks_to_plain_text(&blocks), "first\nlast");
}

// --- CommandResult::Message no regression ---

#[test]
fn command_result_message_to_plain_text_returns_string() {
    let result = CommandResult::Message("hello".into());
    assert_eq!(result.to_plain_text(), Some("hello".into()));
}

// --- CommandResult::Blocks fallback via to_plain_text ---

#[test]
fn command_result_blocks_to_plain_text_uses_output_block_impl() {
    let result = CommandResult::Blocks(vec![
        OutputBlock::text("Status"),
        OutputBlock::section("Runtime", vec![OutputBlock::kv("session_id", "s1")]),
    ]);
    let text = result.to_plain_text().expect("Blocks should produce text");
    assert!(text.contains("Status"), "title block present");
    assert!(text.contains("Runtime:"), "section header present");
    assert!(text.contains("- session_id: s1"), "kv present");
}

#[test]
fn command_result_non_text_variants_return_none() {
    assert_eq!(CommandResult::ContinueToQuery.to_plain_text(), None);
    assert_eq!(
        CommandResult::ContinueToQueryWithPrompt("continue".into()).to_plain_text(),
        None
    );
    assert_eq!(CommandResult::Denied("x".into()).to_plain_text(), None);
}

// --- Status output structure assertions ---

#[test]
fn status_blocks_contain_expected_sections() {
    let blocks = vec![
        OutputBlock::text("Status"),
        OutputBlock::section("Runtime", vec![OutputBlock::kv("session_id", "s1")]),
        OutputBlock::section(
            "Observability",
            vec![OutputBlock::kv("retryable_count", "0")],
        ),
        OutputBlock::section("Commands", vec![OutputBlock::kv("total", "5")]),
        OutputBlock::section(
            "Orchestration",
            vec![OutputBlock::kv("pending_orchestration", "no")],
        ),
        OutputBlock::section(
            "Integrations",
            vec![OutputBlock::kv("mcp_runtime", "unavailable")],
        ),
        OutputBlock::section(
            "Plugins",
            vec![OutputBlock::kv("plugin_discovery", "unavailable")],
        ),
    ];
    let text = blocks_to_plain_text(&blocks);
    for section in &[
        "Runtime:",
        "Observability:",
        "Commands:",
        "Orchestration:",
        "Integrations:",
        "Plugins:",
    ] {
        assert!(text.contains(section), "missing section: {section}");
    }
}

#[test]
fn status_blocks_preserve_field_order() {
    let runtime = OutputBlock::section(
        "Runtime",
        vec![
            OutputBlock::kv("session_id", "s1"),
            OutputBlock::kv("surface", "Cli"),
            OutputBlock::kv("runtime_role", "Primary"),
        ],
    );
    let text = runtime.to_plain_text();
    let sid_pos = text.find("session_id").unwrap();
    let surf_pos = text.find("surface").unwrap();
    let role_pos = text.find("runtime_role").unwrap();
    assert!(sid_pos < surf_pos, "session_id before surface");
    assert!(surf_pos < role_pos, "surface before runtime_role");
}
