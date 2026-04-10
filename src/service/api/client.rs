use std::sync::{Arc, RwLock};

use crate::core::message::Message;
use crate::service::api::streaming::StreamEvent;

#[derive(Debug, Clone, Default)]
pub struct AnthropicClient {
    scripted_turns: Arc<RwLock<Vec<Vec<StreamEvent>>>>,
}

impl AnthropicClient {
    pub fn with_scripted_events(scripted_events: Vec<StreamEvent>) -> Self {
        Self::with_scripted_turns(vec![scripted_events])
    }

    pub fn with_scripted_turns(scripted_turns: Vec<Vec<StreamEvent>>) -> Self {
        Self {
            scripted_turns: Arc::new(RwLock::new(scripted_turns)),
        }
    }

    pub async fn stream_message(&self, _input: &Message) -> Vec<StreamEvent> {
        let mut turns = self
            .scripted_turns
            .write()
            .expect("scripted turns poisoned");
        if turns.is_empty() {
            Vec::new()
        } else {
            turns.remove(0)
        }
    }
}
