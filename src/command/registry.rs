use std::collections::BTreeMap;
use std::sync::Arc;

use crate::command::types::{Command, CommandMetadata};

#[derive(Clone, Default)]
pub struct CommandRegistry {
    commands: BTreeMap<&'static str, Arc<dyn Command>>,
    aliases: BTreeMap<&'static str, &'static str>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, command: Arc<dyn Command>) -> Self {
        let metadata = command.metadata();
        for alias in metadata.aliases {
            self.aliases.insert(alias, metadata.name);
        }
        self.commands.insert(metadata.name, command);
        self
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.resolve_name(name)
            .and_then(|resolved| self.commands.get(resolved).cloned())
    }

    pub fn metadata(&self) -> Vec<CommandMetadata> {
        self.commands
            .values()
            .map(|command| command.metadata())
            .collect()
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.commands.keys().copied().collect()
    }

    fn resolve_name(&self, name: &str) -> Option<&'static str> {
        if self.commands.contains_key(name) {
            Some(self.commands.get_key_value(name)?.0)
        } else {
            self.aliases.get(name).copied()
        }
    }
}
