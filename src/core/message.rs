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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text { text: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub role: Role,
    /// Legacy compat field: always equals the concatenation of all Text blocks.
    /// Kept as a real field so existing `.content` reads compile without changes.
    pub content: String,
    pub blocks: Vec<ContentBlock>,
}

impl Message {
    pub fn user(content: impl Into<String>) -> Self {
        let text = content.into();
        Self {
            role: Role::User,
            blocks: vec![ContentBlock::Text { text: text.clone() }],
            content: text,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        let text = content.into();
        Self {
            role: Role::Assistant,
            blocks: vec![ContentBlock::Text { text: text.clone() }],
            content: text,
        }
    }

    /// Returns all Text block content concatenated.
    pub fn text(&self) -> String {
        self.blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
            })
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn is_text_only(&self) -> bool {
        self.blocks
            .iter()
            .all(|b| matches!(b, ContentBlock::Text { .. }))
    }
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
                let mut content: Option<String> = None;
                let mut blocks: Option<Vec<ContentBlock>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "role" => role = Some(map.next_value()?),
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
                let content_str = resolved_blocks
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                    })
                    .collect::<Vec<_>>()
                    .join("");

                Ok(Message {
                    role,
                    content: content_str,
                    blocks: resolved_blocks,
                })
            }
        }

        deserializer.deserialize_map(MessageVisitor)
    }
}
