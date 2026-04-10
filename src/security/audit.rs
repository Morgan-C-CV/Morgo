#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub category: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    events: Vec<AuditEvent>,
}

impl AuditLog {
    pub fn record(&mut self, category: impl Into<String>, detail: impl Into<String>) {
        self.events.push(AuditEvent {
            category: category.into(),
            detail: detail.into(),
        });
    }

    pub fn events(&self) -> &[AuditEvent] {
        &self.events
    }
}
