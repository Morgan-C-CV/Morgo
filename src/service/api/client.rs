use std::sync::{Arc, RwLock};

use crate::core::message::Message;
use crate::service::api::streaming::{StreamEvent, UsageEvent};

#[derive(Debug, Clone, PartialEq)]
pub struct ModelPricing {
    pub input_per_million_usd: f64,
    pub output_per_million_usd: f64,
    pub cache_write_per_million_usd: f64,
    pub cache_read_per_million_usd: f64,
}

impl Default for ModelPricing {
    fn default() -> Self {
        Self {
            input_per_million_usd: 3.0,
            output_per_million_usd: 15.0,
            cache_write_per_million_usd: 3.75,
            cache_read_per_million_usd: 0.3,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelProviderConfig {
    pub provider_id: String,
    pub base_url: String,
    pub model_id: String,
    pub pricing: ModelPricing,
}

impl Default for ModelProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: "default-provider".into(),
            base_url: "http://localhost".into(),
            model_id: "default-model".into(),
            pricing: ModelPricing::default(),
        }
    }
}

#[derive(Debug, Clone)]
enum ProviderTransport {
    Scripted {
        turns: Arc<RwLock<Vec<Vec<StreamEvent>>>>,
    },
    Production {
        config: ModelProviderConfig,
    },
}

#[derive(Debug, Clone)]
pub struct ModelProviderClient {
    transport: ProviderTransport,
}

impl Default for ModelProviderClient {
    fn default() -> Self {
        Self::from_config(ModelProviderConfig::default())
    }
}

impl ModelProviderClient {
    pub fn with_scripted_events(scripted_events: Vec<StreamEvent>) -> Self {
        Self::with_scripted_turns(vec![scripted_events])
    }

    pub fn with_scripted_turns(scripted_turns: Vec<Vec<StreamEvent>>) -> Self {
        Self {
            transport: ProviderTransport::Scripted {
                turns: Arc::new(RwLock::new(scripted_turns)),
            },
        }
    }

    pub fn from_config(config: ModelProviderConfig) -> Self {
        Self {
            transport: ProviderTransport::Production { config },
        }
    }

    pub fn provider_config(&self) -> ModelProviderConfig {
        match &self.transport {
            ProviderTransport::Scripted { .. } => ModelProviderConfig::default(),
            ProviderTransport::Production { config } => config.clone(),
        }
    }

    pub fn is_scripted(&self) -> bool {
        matches!(self.transport, ProviderTransport::Scripted { .. })
    }

    pub async fn stream_message(&self, input: &Message) -> Vec<StreamEvent> {
        match &self.transport {
            ProviderTransport::Scripted { turns } => {
                let mut turns = turns.write().expect("scripted turns poisoned");
                if turns.is_empty() {
                    Vec::new()
                } else {
                    turns.remove(0)
                }
            }
            ProviderTransport::Production { config } => {
                if input.content.trim().is_empty() {
                    Vec::new()
                } else {
                    vec![
                        StreamEvent::MessageStart,
                        StreamEvent::TextDelta(format!(
                            "production transport placeholder for provider {} model {}",
                            config.provider_id, config.model_id
                        )),
                        StreamEvent::Usage(UsageEvent {
                            model: config.model_id.clone(),
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
