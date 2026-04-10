#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditEvent {
    ToolChecked { tool_name: String },
    ToolDenied { tool_name: String, reason: String },
    TaskStarted { task_id: String },
    TaskFinished { task_id: String, status: String },
    SurfaceDenied { actor_id: String, reason: String },
}

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    events: Vec<AuditEvent>,
}

impl AuditLog {
    pub fn record(&mut self, event: AuditEvent) {
        self.events.push(event);
    }

    pub fn events(&self) -> &[AuditEvent] {
        &self.events
    }
}
