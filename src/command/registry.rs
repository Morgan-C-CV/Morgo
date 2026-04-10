use std::collections::BTreeMap;
use std::sync::Arc;

use crate::command::types::Command;

#[derive(Clone, Default)]
pub struct CommandRegistry {
    commands: BTreeMap<&'static str, Arc<dyn Command>>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, command: Arc<dyn Command>) -> Self {
        self.commands.insert(command.name(), command);
        self
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.commands.get(name).cloned()
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.commands.keys().copied().collect()
    }
}
