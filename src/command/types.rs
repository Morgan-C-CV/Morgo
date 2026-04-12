use async_trait::async_trait;

use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandType {
    Prompt,
    Local,
}

impl CommandType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandAvailability {
    Everywhere,
    CliOnly,
    RemoteSafe,
}

impl CommandAvailability {
    pub fn short_label(&self) -> Option<&'static str> {
        match self {
            Self::Everywhere => None,
            Self::CliOnly => Some("cli-only"),
            Self::RemoteSafe => Some("remote-safe"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandSource {
    Builtin,
    Coding,
    Skill,
    Mcp,
    Plugin,
}

impl CommandSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Coding => "coding",
            Self::Skill => "skill",
            Self::Mcp => "mcp",
            Self::Plugin => "plugin",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Builtin => "Built-in",
            Self::Coding => "Coding",
            Self::Skill => "Skills",
            Self::Mcp => "MCP",
            Self::Plugin => "Plugins",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    pub name: String,
    pub description: String,
    pub source: CommandSource,
    pub category: String,
    pub command_type: CommandType,
    pub availability: CommandAvailability,
    pub aliases: Vec<String>,
    pub is_hidden: bool,
    pub disable_model_invocation: bool,
    pub immediate: bool,
    pub is_sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemTrapAction {
    RequireReboot,
    ResumeSession(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Message(String),
    ContinueToQuery,
    Prompt(String),
    Denied(String),
    UpdateConfig { key: String, value: String },
    SystemTrap(SystemTrapAction),
}

#[async_trait]
pub trait Command: Send + Sync {
    fn metadata(&self) -> CommandMetadata;
    fn is_enabled(&self) -> bool {
        true
    }
    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult>;
}
