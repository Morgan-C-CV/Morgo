use std::sync::{Arc, RwLock};

use crate::core::message::Message;
use crate::service::api::streaming::{StreamEvent, UsageEvent};

#[derive(Debug, Clone)]
enum AnthropicTransport {
    Scripted {
        turns: Arc<RwLock<Vec<Vec<StreamEvent>>>>,
    },
    Production {
        base_url: String,
        model: String,
    },
}

#[derive(Debug, Clone)]
pub struct AnthropicClient {
    transport: AnthropicTransport,
}

impl Default for AnthropicClient {
    fn default() -> Self {
        Self::production_stub()
    }
}

impl AnthropicClient {
    pub fn with_scripted_events(scripted_events: Vec<StreamEvent>) -> Self {
        Self::with_scripted_turns(vec![scripted_events])
    }

    pub fn with_scripted_turns(scripted_turns: Vec<Vec<StreamEvent>>) -> Self {
        Self {
            transport: AnthropicTransport::Scripted {
                turns: Arc::new(RwLock::new(scripted_turns)),
            },
        }
    }

    pub fn production_stub() -> Self {
        Self {
            transport: AnthropicTransport::Production {
                base_url: "https://api.anthropic.com".into(),
                model: "claude-sonnet-4-6".into(),
            },
        }
    }

    pub fn is_scripted(&self) -> bool {
        matches!(self.transport, AnthropicTransport::Scripted { .. })
    }

    pub async fn stream_message(&self, input: &Message) -> Vec<StreamEvent> {
        match &self.transport {
            AnthropicTransport::Scripted { turns } => {
                let mut turns = turns.write().expect("scripted turns poisoned");
                if turns.is_empty() {
                    Vec::new()
                } else {
                    turns.remove(0)
                }
            }
            AnthropicTransport::Production { model, .. } => {
                if input.content.trim().is_empty() {
                    Vec::new()
                } else {
                    vec![
                        StreamEvent::MessageStart,
                        StreamEvent::TextDelta(format!(
                            "production transport placeholder for model {}",
                            model
                        )),
                        StreamEvent::Usage(UsageEvent {
                            model: model.clone(),
                            input_tokens: input.content.len(),
                            output_tokens: 0,
                            cache_creation_input_tokens: 0,
                            cache_read_input_tokens: 0,
                        }),
                        StreamEvent::MessageStop {
                            stop_reason: crate::service::api::streaming::StopReason::EndTurn,
                        },
                    ]
                }
            }
        }
    }
}
