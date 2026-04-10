use crate::core::message::Message;
use crate::service::api::streaming::StreamEvent;

#[derive(Debug, Clone, Default)]
pub struct AnthropicClient {
    scripted_events: Vec<StreamEvent>,
}

impl AnthropicClient {
    pub fn with_scripted_events(scripted_events: Vec<StreamEvent>) -> Self {
        Self { scripted_events }
    }

    pub async fn stream_message(&self, _input: &Message) -> Vec<StreamEvent> {
        self.scripted_events.clone()
    }
}
