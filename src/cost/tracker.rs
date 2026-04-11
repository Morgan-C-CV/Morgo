use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default)]
pub struct CostTracker {
    inner: Arc<RwLock<CostState>>,
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
    pub fn record_request(&self, input_tokens: usize, output_tokens: usize) {
        self.record_model_usage("unknown", input_tokens, output_tokens, 0, 0);
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
        let estimated_cost_usd = estimate_cost_usd(
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
}

fn estimate_cost_usd(
    model: &str,
    input_tokens: usize,
    output_tokens: usize,
    cache_creation_input_tokens: usize,
    cache_read_input_tokens: usize,
) -> f64 {
    let (input_rate, output_rate, cache_write_rate, cache_read_rate) = match model {
        "claude-opus-4-6" => (15.0, 75.0, 18.75, 1.5),
        "claude-haiku-4-5" => (0.8, 4.0, 1.0, 0.08),
        _ => (3.0, 15.0, 3.75, 0.3),
    };
    (input_tokens as f64 / 1_000_000.0) * input_rate
        + (output_tokens as f64 / 1_000_000.0) * output_rate
        + (cache_creation_input_tokens as f64 / 1_000_000.0) * cache_write_rate
        + (cache_read_input_tokens as f64 / 1_000_000.0) * cache_read_rate
}
