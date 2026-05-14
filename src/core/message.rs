use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageVisibility {
    #[default]
    Visible,
    ToolScaffold,
    RuntimeMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text { text: String },
    Image { media_type: String, data: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub role: Role,
    /// Legacy compat field: always equals the concatenation of all Text blocks.
    /// Kept as a real field so existing `.content` reads compile without changes.
    pub content: String,
    pub blocks: Vec<ContentBlock>,
    pub visibility: MessageVisibility,
}

impl Message {
    pub fn user(content: impl Into<String>) -> Self {
        Self::user_with_visibility(content, MessageVisibility::Visible)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::assistant_with_visibility(content, MessageVisibility::Visible)
    }

    pub fn user_with_visibility(content: impl Into<String>, visibility: MessageVisibility) -> Self {
        let text = content.into();
        Self {
            role: Role::User,
            blocks: vec![ContentBlock::Text { text: text.clone() }],
            content: text,
            visibility,
        }
    }

    pub fn assistant_with_visibility(
        content: impl Into<String>,
        visibility: MessageVisibility,
    ) -> Self {
        let text = content.into();
        Self {
            role: Role::Assistant,
            blocks: vec![ContentBlock::Text { text: text.clone() }],
            content: text,
            visibility,
        }
    }

    pub fn from_blocks(role: Role, blocks: Vec<ContentBlock>) -> Self {
        Self::from_blocks_with_visibility(role, blocks, MessageVisibility::Visible)
    }

    pub fn from_blocks_with_visibility(
        role: Role,
        blocks: Vec<ContentBlock>,
        visibility: MessageVisibility,
    ) -> Self {
        let content = text_from_blocks(&blocks);
        Self {
            role,
            content,
            blocks,
            visibility,
        }
    }

    pub fn with_visibility(mut self, visibility: MessageVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    /// Returns all Text block content concatenated. Image blocks are skipped.
    pub fn text(&self) -> String {
        text_from_blocks(&self.blocks)
    }

    pub fn is_text_only(&self) -> bool {
        self.blocks
            .iter()
            .all(|b| matches!(b, ContentBlock::Text { .. }))
    }

    pub fn is_visible_to_user(&self) -> bool {
        self.visibility == MessageVisibility::Visible
            && !is_legacy_hidden_message_text(&self.text())
    }

    pub fn has_visible_text(&self) -> bool {
        self.is_visible_to_user() && !self.text().trim().is_empty()
    }
}

pub fn is_legacy_hidden_primary_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("tool batch result:")
        || lower.starts_with("tool batch failed:")
        || lower.starts_with("tool result for ")
        || lower.starts_with("tool progress for ")
        || lower.starts_with("verified_target:")
        || lower.starts_with("verification_result:")
        || lower.starts_with("minimal_evidence:")
        || lower.starts_with("remaining_blocker:")
        || (lower.starts_with("tool ")
            && (lower.contains(" result:")
                || lower.contains(" denied:")
                || lower.contains(" denied by hook:")
                || lower.contains(" denied before execution:")
                || lower.contains(" interrupted:")
                || lower.contains(" progress:")
                || lower.contains(" failed:")
                || lower.contains(" result too large:")
                || lower.contains(" oversized result preserved:")
                || lower.contains(" structured failure preserved:")
                || lower.contains(" result missing")))
}

pub fn is_legacy_hidden_message_text(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("tool result for ")
        || lower.starts_with("tool progress for ")
        || lower.starts_with("tool batch result:")
        || lower.starts_with("tool batch failed:")
        || lower.starts_with("verified_target:")
        || lower.starts_with("verification_result:")
        || lower.starts_with("minimal_evidence:")
        || lower.starts_with("remaining_blocker:")
    {
        return true;
    }

    if lower.starts_with("tool ")
        && (lower.contains(" result:")
            || lower.contains(" denied:")
            || lower.contains(" denied by hook:")
            || lower.contains(" denied before execution:")
            || lower.contains(" interrupted:")
            || lower.contains(" progress:")
            || lower.contains(" failed:")
            || lower.contains(" result too large:")
            || lower.contains(" oversized result preserved:")
            || lower.contains(" structured failure preserved:")
            || lower.contains(" result missing"))
    {
        return true;
    }

    let non_empty_lines = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    !non_empty_lines.is_empty()
        && non_empty_lines
            .iter()
            .all(|line| is_legacy_hidden_primary_line(line))
}

fn text_from_blocks(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            ContentBlock::Image { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

// --- Custom serde ---
//
// Serialize: emit both `blocks` (new format) and a derived `content` string
// (legacy compat) so existing readers that assert on `content` keep working.
// Once all read paths are confirmed migrated, drop the `content` output.
//
// Deserialize: prefer `blocks` when present; fall back to `content` string
// (old format) by converting it to a single Text block.

impl Serialize for Message {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("role", &self.role)?;
        map.serialize_entry("visibility", &self.visibility)?;
        // Legacy compat: derive content string from blocks
        let content_str = self.text();
        map.serialize_entry("content", &content_str)?;
        map.serialize_entry("blocks", &self.blocks)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a Message object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Message, A::Error> {
                let mut role: Option<Role> = None;
                let mut visibility: Option<MessageVisibility> = None;
                let mut content: Option<String> = None;
                let mut blocks: Option<Vec<ContentBlock>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "role" => role = Some(map.next_value()?),
                        "visibility" => visibility = Some(map.next_value()?),
                        "content" => content = Some(map.next_value()?),
                        "blocks" => blocks = Some(map.next_value()?),
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                let role = role.ok_or_else(|| de::Error::missing_field("role"))?;

                // blocks takes priority; fall back to legacy content string
                let resolved_blocks = if let Some(b) = blocks {
                    b
                } else if let Some(ref c) = content {
                    vec![ContentBlock::Text { text: c.clone() }]
                } else {
                    vec![]
                };

                // Keep content field in sync with blocks
                let content_str = text_from_blocks(&resolved_blocks);

                Ok(Message {
                    role,
                    content: content_str,
                    blocks: resolved_blocks,
                    visibility: visibility.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(MessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, MessageVisibility, Role, is_legacy_hidden_message_text};

    #[test]
    fn legacy_messages_default_visibility_to_visible() {
        let message = serde_json::from_str::<Message>(
            r#"{"role":"Assistant","content":"hello","blocks":[{"type":"text","text":"hello"}]}"#,
        )
        .expect("deserialize legacy message");

        assert_eq!(message.visibility, MessageVisibility::Visible);
        assert!(message.is_visible_to_user());
    }

    #[test]
    fn serialize_includes_visibility_field() {
        let message = Message::assistant_with_visibility(
            "tool Read result: Read succeeded",
            MessageVisibility::ToolScaffold,
        );
        let json = serde_json::to_value(&message).expect("serialize message");

        assert_eq!(json["visibility"], "tool_scaffold");
    }

    #[test]
    fn legacy_tool_scaffold_text_is_hidden_from_user_views() {
        let message = Message::assistant("tool result for Read:\nverified_target: /tmp/report.md");

        assert!(is_legacy_hidden_message_text(&message.text()));
        assert!(!message.is_visible_to_user());
    }

    #[test]
    fn image_block_construction_keeps_text_compat_field() {
        let message = Message::from_blocks(
            Role::User,
            vec![
                super::ContentBlock::Text {
                    text: "describe this".into(),
                },
                super::ContentBlock::Image {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                },
            ],
        );

        assert_eq!(message.content, "describe this");
        assert_eq!(message.text(), "describe this");
    }
}
