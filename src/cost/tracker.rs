use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default)]
pub struct CostTracker {
    inner: Arc<RwLock<CostState>>,
}

#[derive(Debug, Default)]
struct CostState {
    requests: usize,
    input_tokens: usize,
    output_tokens: usize,
}

impl CostTracker {
    pub fn record_request(&self, input_tokens: usize, output_tokens: usize) {
        let mut state = self.inner.write().expect("cost tracker poisoned");
        state.requests += 1;
        state.input_tokens += input_tokens;
        state.output_tokens += output_tokens;
    }

    pub fn format_report(&self) -> String {
        let state = self.inner.read().expect("cost tracker poisoned");
        format!(
            "Session cost summary\nrequests: {}\ninput_tokens: {}\noutput_tokens: {}\nstatus: partial accounting",
            state.requests, state.input_tokens, state.output_tokens
        )
    }
}
