use std::collections::BTreeMap;
use std::sync::Arc;

use crate::command::types::{Command, CommandMetadata};

#[derive(Clone, Default)]
pub struct CommandRegistry {
    commands: BTreeMap<String, Arc<dyn Command>>,
    aliases: BTreeMap<String, String>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, command: Arc<dyn Command>) -> Self {
        let metadata = command.metadata();
        let command_name = metadata.name.clone();
        assert!(
            !self.commands.contains_key(&command_name),
            "duplicate command registration for '{}' from source {}",
            metadata.name,
            metadata.source.as_str()
        );
        assert!(
            !self.aliases.contains_key(&command_name),
            "command '{}' conflicts with an alias from source {}",
            metadata.name,
            metadata.source.as_str()
        );
        for alias in &metadata.aliases {
            assert!(
                !self.commands.contains_key(alias),
                "alias '{}' conflicts with command '{}' from source {}",
                alias,
                metadata.name,
                metadata.source.as_str()
            );
            let previous = self.aliases.insert(alias.clone(), command_name.clone());
            assert!(
                previous.is_none(),
                "duplicate alias '{}' while registering command '{}' from source {}",
                alias,
                metadata.name,
                metadata.source.as_str()
            );
        }
        self.commands.insert(command_name, command);
        self
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Command>> {
        self.resolve_name(name)
            .and_then(|resolved| self.commands.get(&resolved).cloned())
    }

    pub fn metadata(&self) -> Vec<CommandMetadata> {
        self.commands
            .values()
            .map(|command| command.metadata())
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.commands.keys().cloned().collect()
    }

    fn resolve_name(&self, name: &str) -> Option<String> {
        if self.commands.contains_key(name) {
            Some(name.to_string())
        } else {
            self.aliases.get(name).cloned()
        }
    }
}
