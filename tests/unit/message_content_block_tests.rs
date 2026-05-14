use rust_agent::core::message::{ContentBlock, Message, Role};

#[test]
fn user_constructor_produces_single_text_block() {
    let msg = Message::user("hello");
    assert_eq!(msg.role, Role::User);
    assert_eq!(msg.blocks.len(), 1);
    assert!(matches!(&msg.blocks[0], ContentBlock::Text { text } if text == "hello"));
}

#[test]
fn assistant_constructor_produces_single_text_block() {
    let msg = Message::assistant("world");
    assert_eq!(msg.role, Role::Assistant);
    assert_eq!(msg.blocks.len(), 1);
    assert!(matches!(&msg.blocks[0], ContentBlock::Text { text } if text == "world"));
}

#[test]
fn text_helper_returns_concatenated_text_blocks() {
    let msg = Message::user("hello");
    assert_eq!(msg.text(), "hello");
}

#[test]
fn text_helper_on_empty_blocks_returns_empty_string() {
    let msg = Message::from_blocks(Role::User, vec![]);
    assert_eq!(msg.text(), "");
}

#[test]
fn is_text_only_true_for_pure_text_message() {
    let msg = Message::user("hello");
    assert!(msg.is_text_only());
}

#[test]
fn is_text_only_true_for_empty_blocks() {
    let msg = Message::from_blocks(Role::User, vec![]);
    assert!(msg.is_text_only());
}

#[test]
fn serde_old_format_content_string_deserializes_to_text_block() {
    let json = r#"{"role":"User","content":"legacy text"}"#;
    let msg: Message = serde_json::from_str(json).expect("old format should deserialize");
    assert_eq!(msg.text(), "legacy text");
    assert_eq!(msg.blocks.len(), 1);
    assert!(matches!(&msg.blocks[0], ContentBlock::Text { text } if text == "legacy text"));
}

#[test]
fn serde_new_format_blocks_deserializes_correctly() {
    let json = r#"{"role":"User","blocks":[{"type":"text","text":"new format"}]}"#;
    let msg: Message = serde_json::from_str(json).expect("new format should deserialize");
    assert_eq!(msg.text(), "new format");
}

#[test]
fn serde_blocks_takes_priority_over_content_when_both_present() {
    let json = r#"{"role":"User","content":"old","blocks":[{"type":"text","text":"new"}]}"#;
    let msg: Message = serde_json::from_str(json).expect("should deserialize");
    assert_eq!(msg.text(), "new");
}

#[test]
fn serde_serialize_emits_both_content_and_blocks_for_compat() {
    let msg = Message::user("hello");
    let json = serde_json::to_string(&msg).expect("should serialize");
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["content"], "hello");
    assert_eq!(value["blocks"][0]["type"], "text");
    assert_eq!(value["blocks"][0]["text"], "hello");
}

#[test]
fn serde_round_trip_preserves_text() {
    let original = Message::assistant("round trip");
    let json = serde_json::to_string(&original).expect("should serialize");
    let restored: Message = serde_json::from_str(&json).expect("should deserialize");
    assert_eq!(restored.text(), "round trip");
    assert_eq!(restored.role, Role::Assistant);
}
