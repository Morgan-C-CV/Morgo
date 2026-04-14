use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::service::api::client::ModelPricing;

#[derive(Debug, Clone)]
pub struct CostTracker {
    inner: Arc<RwLock<CostState>>,
    pricing_catalog: Arc<RwLock<BTreeMap<String, ModelPricing>>>,
    default_model_id: Arc<RwLock<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CostSnapshot {
    pub requests: usize,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
    pub estimated_cost_micros_usd: u64,
}

impl CostSnapshot {
    pub fn delta_since(&self, before: &Self) -> Self {
        Self {
            requests: self.requests.saturating_sub(before.requests),
            input_tokens: self.input_tokens.saturating_sub(before.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(before.output_tokens),
            cache_creation_input_tokens: self
                .cache_creation_input_tokens
                .saturating_sub(before.cache_creation_input_tokens),
            cache_read_input_tokens: self
                .cache_read_input_tokens
                .saturating_sub(before.cache_read_input_tokens),
            estimated_cost_micros_usd: self
                .estimated_cost_micros_usd
                .saturating_sub(before.estimated_cost_micros_usd),
        }
    }

    pub fn has_usage(&self) -> bool {
        self.requests > 0
            || self.input_tokens > 0
            || self.output_tokens > 0
            || self.cache_creation_input_tokens > 0
            || self.cache_read_input_tokens > 0
            || self.estimated_cost_micros_usd > 0
    }
}

impl Default for CostTracker {
    fn default() -> Self {
        Self::with_default_pricing("default-model".into(), ModelPricing::default())
    }
}

#[derive(Debug, Clone, Default)]
struct ModelUsage {
    requests: usize,
    input_tokens: usize,
    output_tokens: usize,
    cache_creation_input_tokens: usize,
    cache_read_input_tokens: usize,
    estimated_cost_usd: f64,
}

#[derive(Debug, Default)]
struct CostState {
    requests: usize,
    input_tokens: usize,
    output_tokens: usize,
    cache_creation_input_tokens: usize,
    cache_read_input_tokens: usize,
    estimated_cost_usd: f64,
    by_model: BTreeMap<String, ModelUsage>,
}

impl CostTracker {
    pub fn with_default_pricing(model_id: String, pricing: ModelPricing) -> Self {
        let mut pricing_catalog = BTreeMap::new();
        pricing_catalog.insert(model_id.clone(), pricing);
        Self {
            inner: Arc::new(RwLock::new(CostState::default())),
            pricing_catalog: Arc::new(RwLock::new(pricing_catalog)),
            default_model_id: Arc::new(RwLock::new(model_id)),
        }
    }

    pub fn register_model_pricing(&self, model_id: impl Into<String>, pricing: ModelPricing) {
        let model_id = model_id.into();
        self.pricing_catalog
            .write()
            .expect("cost tracker pricing catalog poisoned")
            .insert(model_id, pricing);
    }

    pub fn record_request(&self, input_tokens: usize, output_tokens: usize) {
        let default_model_id = self
            .default_model_id
            .read()
            .expect("cost tracker default model poisoned")
            .clone();
        self.record_model_usage(&default_model_id, input_tokens, output_tokens, 0, 0);
    }

    pub fn record_model_usage(
        &self,
        model: &str,
        input_tokens: usize,
        output_tokens: usize,
        cache_creation_input_tokens: usize,
        cache_read_input_tokens: usize,
    ) {
        let mut state = self.inner.write().expect("cost tracker poisoned");
        let estimated_cost_usd = self.estimate_cost_usd(
            model,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
        );
        state.requests += 1;
        state.input_tokens += input_tokens;
        state.output_tokens += output_tokens;
        state.cache_creation_input_tokens += cache_creation_input_tokens;
        state.cache_read_input_tokens += cache_read_input_tokens;
        state.estimated_cost_usd += estimated_cost_usd;

        let model_usage = state.by_model.entry(model.to_string()).or_default();
        model_usage.requests += 1;
        model_usage.input_tokens += input_tokens;
        model_usage.output_tokens += output_tokens;
        model_usage.cache_creation_input_tokens += cache_creation_input_tokens;
        model_usage.cache_read_input_tokens += cache_read_input_tokens;
        model_usage.estimated_cost_usd += estimated_cost_usd;
    }

    pub fn snapshot(&self) -> CostSnapshot {
        let state = self.inner.read().expect("cost tracker poisoned");
        CostSnapshot {
            requests: state.requests,
            input_tokens: state.input_tokens,
            output_tokens: state.output_tokens,
            cache_creation_input_tokens: state.cache_creation_input_tokens,
            cache_read_input_tokens: state.cache_read_input_tokens,
            estimated_cost_micros_usd: (state.estimated_cost_usd * 1_000_000.0).round() as u64,
        }
    }

    pub fn format_report(&self) -> String {
        let state = self.inner.read().expect("cost tracker poisoned");
        let mut lines = vec![
            "Session cost summary".into(),
            format!("requests: {}", state.requests),
            format!("input_tokens: {}", state.input_tokens),
            format!("output_tokens: {}", state.output_tokens),
            format!(
                "cache_creation_input_tokens: {}",
                state.cache_creation_input_tokens
            ),
            format!("cache_read_input_tokens: {}", state.cache_read_input_tokens),
            format!("estimated_cost_usd: {:.6}", state.estimated_cost_usd),
        ];
        for (model, usage) in &state.by_model {
            lines.push(format!(
                "model {} -> requests: {}, input_tokens: {}, output_tokens: {}, cache_creation_input_tokens: {}, cache_read_input_tokens: {}, estimated_cost_usd: {:.6}",
                model,
                usage.requests,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
                usage.estimated_cost_usd
            ));
        }
        lines.join("\n")
    }

    fn estimate_cost_usd(
        &self,
        model: &str,
        input_tokens: usize,
        output_tokens: usize,
        cache_creation_input_tokens: usize,
        cache_read_input_tokens: usize,
    ) -> f64 {
        let pricing_catalog = self
            .pricing_catalog
            .read()
            .expect("cost tracker pricing catalog poisoned");
        let pricing = pricing_catalog
            .get(model)
            .cloned()
            .or_else(|| pricing_catalog.values().next().cloned())
            .unwrap_or_default();
        (input_tokens as f64 / 1_000_000.0) * pricing.input_per_million_usd
            + (output_tokens as f64 / 1_000_000.0) * pricing.output_per_million_usd
            + (cache_creation_input_tokens as f64 / 1_000_000.0)
                * pricing.cache_write_per_million_usd
            + (cache_read_input_tokens as f64 / 1_000_000.0) * pricing.cache_read_per_million_usd
    }
}
