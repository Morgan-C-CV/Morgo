use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Deserialize;

use crate::bootstrap::config_root::preferred_workspace_config_root;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookEvent {
    SessionStart,
    Setup,
    UserPromptSubmit,
    PreToolUse {
        tool_name: String,
    },
    PostToolUse {
        tool_name: String,
    },
    PostToolUseFailure {
        tool_name: String,
    },
    PermissionRequest {
        tool_name: String,
    },
    PermissionDenied {
        tool_name: String,
        reason: String,
    },
    Stop,
    SubagentStop,
    Notification {
        title: String,
        body: String,
        notification_type: String,
        task_id: Option<String>,
        task_type: Option<String>,
        status: Option<String>,
        output_file: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRule {
    pub event: HookEventMatcher,
    pub layer: HookRuleLayer,
    pub deny_match: Option<String>,
    pub append_message: Option<String>,
    pub prevent_continuation: bool,
    pub block_continuation: bool,
    pub permission_decision: Option<String>,
    pub updated_input: Option<String>,
    pub additional_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookEventMatcher {
    SessionStart,
    Setup,
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    PermissionRequest,
    PermissionDenied,
    Stop,
    SubagentStop,
    Notification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookRuleLayer {
    Defaults,
    File,
    Plugin,
    Runtime,
}

impl HookRuleLayer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
            Self::Plugin => "plugin",
            Self::Runtime => "runtime",
        }
    }

    pub fn precedence(&self) -> u8 {
        match self {
            Self::Defaults => 0,
            Self::File => 1,
            Self::Plugin => 2,
            Self::Runtime => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookConfigLoadResult {
    pub path: PathBuf,
    pub source: HookConfigSource,
    pub rules: Vec<HookRule>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookConfigSource {
    Defaults,
    File,
}

impl HookConfigSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HookRegistry {
    rules: Vec<HookRule>,
    events: Arc<RwLock<Vec<HookEvent>>>,
    config_load_result: Option<HookConfigLoadResult>,
}

impl HookRegistry {
    pub fn register_rule(mut self, rule: HookRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn from_rules(rules: Vec<HookRule>) -> Self {
        Self {
            rules,
            events: Arc::new(RwLock::new(Vec::new())),
            config_load_result: None,
        }
    }

    pub fn with_config_load_result(mut self, config_load_result: HookConfigLoadResult) -> Self {
        self.config_load_result = Some(config_load_result);
        self
    }

    pub fn config_load_result(&self) -> Option<&HookConfigLoadResult> {
        self.config_load_result.as_ref()
    }

    pub fn rules(&self) -> &[HookRule] {
        &self.rules
    }

    pub fn record(&self, event: HookEvent) {
        self.events
            .write()
            .expect("hook events poisoned")
            .push(event);
    }

    pub fn recorded_events(&self) -> Vec<HookEvent> {
        self.events.read().expect("hook events poisoned").clone()
    }
}

pub fn load_hook_registry(cwd: &Path) -> HookRegistry {
    let result = load_hook_rules_with_diagnostics(cwd);
    HookRegistry::from_rules(result.rules.clone()).with_config_load_result(result)
}

pub fn load_hook_registry_from_root(config_root: &Path) -> HookRegistry {
    let result = load_hook_rules_from_root(config_root);
    HookRegistry::from_rules(result.rules.clone()).with_config_load_result(result)
}

pub fn load_hook_rules_with_diagnostics(cwd: &Path) -> HookConfigLoadResult {
    load_hook_rules_from_root(&preferred_workspace_config_root(cwd))
}

pub fn load_hook_rules_from_root(config_root: &Path) -> HookConfigLoadResult {
    let path = config_root.join("hooks.json");
    let mut diagnostics = Vec::new();

    match std::fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Vec<HookRuleConfig>>(&raw) {
            Ok(configs) if !configs.is_empty() => {
                let mut rules = Vec::new();
                for config in configs {
                    match config.into_rule_with_diagnostics() {
                        Ok(rule) => rules.push(rule),
                        Err(diagnostic) => diagnostics.push(diagnostic),
                    }
                }
                diagnostics.push(format!(
                    "Loaded {} hook rule(s) from {} (layer=file).",
                    rules.len(),
                    path.display()
                ));
                HookConfigLoadResult {
                    path,
                    source: HookConfigSource::File,
                    rules,
                    diagnostics,
                }
            }
            Ok(_) => {
                diagnostics
                    .push("Hook config file was empty; using no external hook rules.".to_string());
                HookConfigLoadResult {
                    path,
                    source: HookConfigSource::Defaults,
                    rules: Vec::new(),
                    diagnostics,
                }
            }
            Err(error) => {
                diagnostics.push(format!(
                    "Failed to parse {}: {error}; using no external hook rules.",
                    path.display()
                ));
                HookConfigLoadResult {
                    path,
                    source: HookConfigSource::Defaults,
                    rules: Vec::new(),
                    diagnostics,
                }
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            diagnostics.push(format!(
                "No {} found; using no external hook rules.",
                path.display()
            ));
            HookConfigLoadResult {
                path,
                source: HookConfigSource::Defaults,
                rules: Vec::new(),
                diagnostics,
            }
        }
        Err(error) => {
            diagnostics.push(format!(
                "Failed to read {}: {error}; using no external hook rules.",
                path.display()
            ));
            HookConfigLoadResult {
                path,
                source: HookConfigSource::Defaults,
                rules: Vec::new(),
                diagnostics,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct HookRuleConfig {
    event: String,
    #[serde(default)]
    deny_match: Option<String>,
    #[serde(default)]
    append_message: Option<String>,
    #[serde(default)]
    prevent_continuation: bool,
    #[serde(default)]
    block_continuation: bool,
    #[serde(default)]
    permission_decision: Option<String>,
    #[serde(default)]
    updated_input: Option<String>,
    #[serde(default)]
    additional_context: Option<String>,
}

impl HookRuleConfig {
    fn into_rule_with_diagnostics(self) -> Result<HookRule, String> {
        let event_value = self.event.clone();
        let event = parse_event_matcher(&event_value).ok_or_else(|| {
            format!("Ignored hook rule with unknown event '{event_value}' in hooks.json")
        })?;
        Ok(HookRule {
            event,
            layer: HookRuleLayer::File,
            deny_match: self.deny_match,
            append_message: self.append_message,
            prevent_continuation: self.prevent_continuation,
            block_continuation: self.block_continuation,
            permission_decision: self.permission_decision,
            updated_input: self.updated_input,
            additional_context: self.additional_context,
        })
    }
}

fn parse_event_matcher(value: &str) -> Option<HookEventMatcher> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sessionstart" | "session_start" => Some(HookEventMatcher::SessionStart),
        "setup" => Some(HookEventMatcher::Setup),
        "userpromptsubmit" | "user_prompt_submit" => Some(HookEventMatcher::UserPromptSubmit),
        "pretooluse" | "pre_tool_use" => Some(HookEventMatcher::PreToolUse),
        "posttooluse" | "post_tool_use" => Some(HookEventMatcher::PostToolUse),
        "posttoolusefailure" | "post_tool_use_failure" => {
            Some(HookEventMatcher::PostToolUseFailure)
        }
        "permissionrequest" | "permission_request" => Some(HookEventMatcher::PermissionRequest),
        "permissiondenied" | "permission_denied" => Some(HookEventMatcher::PermissionDenied),
        "stop" => Some(HookEventMatcher::Stop),
        "subagentstop" | "subagent_stop" => Some(HookEventMatcher::SubagentStop),
        "notification" => Some(HookEventMatcher::Notification),
        _ => None,
    }
}
