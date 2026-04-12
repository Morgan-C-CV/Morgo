use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Deserialize;

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
        status: Option<String>,
        output_file: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRule {
    pub event: HookEventMatcher,
    pub deny_match: Option<String>,
    pub append_message: Option<String>,
    pub prevent_continuation: bool,
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

pub fn load_hook_rules_with_diagnostics(cwd: &Path) -> HookConfigLoadResult {
    let path = cwd.join(".claude").join("hooks.json");
    let mut diagnostics = Vec::new();

    match std::fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Vec<HookRuleConfig>>(&raw) {
            Ok(configs) if !configs.is_empty() => HookConfigLoadResult {
                path,
                source: HookConfigSource::File,
                rules: configs.into_iter().map(HookRuleConfig::into_rule).collect(),
                diagnostics,
            },
            Ok(_) => {
                diagnostics.push("Hook config file was empty; using no external hook rules.".to_string());
                HookConfigLoadResult {
                    path,
                    source: HookConfigSource::Defaults,
                    rules: Vec::new(),
                    diagnostics,
                }
            }
            Err(error) => {
                diagnostics.push(format!(
                    "Failed to parse .claude/hooks.json: {error}; using no external hook rules."
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
            diagnostics.push("No .claude/hooks.json found; using no external hook rules.".to_string());
            HookConfigLoadResult {
                path,
                source: HookConfigSource::Defaults,
                rules: Vec::new(),
                diagnostics,
            }
        }
        Err(error) => {
            diagnostics.push(format!(
                "Failed to read .claude/hooks.json: {error}; using no external hook rules."
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
    permission_decision: Option<String>,
    #[serde(default)]
    updated_input: Option<String>,
    #[serde(default)]
    additional_context: Option<String>,
}

impl HookRuleConfig {
    fn into_rule(self) -> HookRule {
        HookRule {
            event: parse_event_matcher(&self.event),
            deny_match: self.deny_match,
            append_message: self.append_message,
            prevent_continuation: self.prevent_continuation,
            permission_decision: self.permission_decision,
            updated_input: self.updated_input,
            additional_context: self.additional_context,
        }
    }
}

fn parse_event_matcher(value: &str) -> HookEventMatcher {
    match value.trim().to_ascii_lowercase().as_str() {
        "sessionstart" | "session_start" => HookEventMatcher::SessionStart,
        "setup" => HookEventMatcher::Setup,
        "userpromptsubmit" | "user_prompt_submit" => HookEventMatcher::UserPromptSubmit,
        "pretooluse" | "pre_tool_use" => HookEventMatcher::PreToolUse,
        "posttooluse" | "post_tool_use" => HookEventMatcher::PostToolUse,
        "posttoolusefailure" | "post_tool_use_failure" => HookEventMatcher::PostToolUseFailure,
        "permissionrequest" | "permission_request" => HookEventMatcher::PermissionRequest,
        "permissiondenied" | "permission_denied" => HookEventMatcher::PermissionDenied,
        "stop" => HookEventMatcher::Stop,
        "subagentstop" | "subagent_stop" => HookEventMatcher::SubagentStop,
        "notification" => HookEventMatcher::Notification,
        _ => HookEventMatcher::Setup,
    }
}
