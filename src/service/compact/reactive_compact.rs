#[derive(Debug, Clone, Default)]
pub struct ReactiveCompactor;

impl ReactiveCompactor {
    pub fn should_compact(&self, token_estimate: usize, limit: usize) -> bool {
        token_estimate >= limit
    }
}
