use crate::service::api::errors::ApiError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 200,
            max_backoff_ms: 1_000,
        }
    }
}

impl RetryPolicy {
    pub fn should_retry(&self, attempt: usize, error: &ApiError, saw_stream_event: bool) -> bool {
        if saw_stream_event {
            return false;
        }
        attempt + 1 < self.max_attempts && error.disposition.is_pre_stream_retryable()
    }

    pub fn backoff_for_attempt(&self, attempt: usize) -> std::time::Duration {
        let exponent = attempt.min(10) as u32;
        let scaled = self.initial_backoff_ms.saturating_mul(1_u64 << exponent);
        std::time::Duration::from_millis(scaled.min(self.max_backoff_ms))
    }
}
