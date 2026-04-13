use crate::core::events::SessionMilestone;
use crate::core::message::Message;
use crate::history::session::{SessionHistory, SessionHistoryEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptEntry {
    pub message: Message,
    pub milestone: Option<SessionMilestone>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transcript {
    pub entries: Vec<TranscriptEntry>,
}

impl Transcript {
    pub fn push(&mut self, message: Message) {
        self.entries.push(TranscriptEntry {
            message,
            milestone: None,
        });
    }

    pub fn messages(&self) -> Vec<Message> {
        self.entries
            .iter()
            .map(|entry| entry.message.clone())
            .collect()
    }
}

impl From<SessionHistory> for Transcript {
    fn from(history: SessionHistory) -> Self {
        Self {
            entries: history
                .entries
                .into_iter()
                .map(|entry| TranscriptEntry {
                    message: entry.message,
                    milestone: entry.milestone,
                })
                .collect(),
        }
    }
}

impl From<Transcript> for SessionHistory {
    fn from(transcript: Transcript) -> Self {
        Self {
            entries: transcript
                .entries
                .into_iter()
                .map(|entry| SessionHistoryEntry {
                    message: entry.message,
                    timestamp: None,
                    tool_refs: Vec::new(),
                    milestone: entry.milestone,
                })
                .collect(),
        }
    }
}
