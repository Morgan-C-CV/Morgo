use crate::core::message::Message;
use crate::history::session::{SessionHistory, SessionHistoryEntry};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transcript {
    pub messages: Vec<Message>,
}

impl Transcript {
    pub fn push(&mut self, message: Message) {
        self.messages.push(message);
    }
}

impl From<SessionHistory> for Transcript {
    fn from(history: SessionHistory) -> Self {
        Self {
            messages: history
                .entries
                .into_iter()
                .map(|entry| entry.message)
                .collect(),
        }
    }
}

impl From<Transcript> for SessionHistory {
    fn from(transcript: Transcript) -> Self {
        Self {
            entries: transcript
                .messages
                .into_iter()
                .map(|message| SessionHistoryEntry {
                    message,
                    timestamp: None,
                    tool_refs: Vec::new(),
                })
                .collect(),
        }
    }
}
