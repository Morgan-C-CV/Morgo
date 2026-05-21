use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::security::workspace_capability::{
    WorkspacePermissionConfig, WorkspacePermissionLevel, default_workspace_permissions_path,
    load_global_workspace_permissions, save_global_workspace_permissions,
};
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;

pub struct PermissionsCommand;

#[async_trait]
impl Command for PermissionsCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "permissions".into(),
            description: "Inspect and update permission mode and explicit tool rules".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["perms".into()],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: true,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();
        if args.is_empty() {
            return Ok(CommandResult::Message(render_permissions_summary(
                app_state,
            )));
        }

        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or_default();
        match action {
            "mode" => {
                let Some(raw_mode) = parts.next() else {
                    anyhow::bail!("usage: /permissions mode <default|plan|accept-edits|bypass>");
                };
                let mode = parse_mode(raw_mode)?;
                app_state.permission_context.set_mode(mode);
                Ok(CommandResult::Message(format!(
                    "Permission mode set to {}.",
                    format_mode(mode)
                )))
            }
            "allow" => update_rule_list(app_state, RuleList::Allow, parts.collect()),
            "deny" => update_rule_list(app_state, RuleList::Deny, parts.collect()),
            "ask" => update_rule_list(app_state, RuleList::Ask, parts.collect()),
            "trust" => trust_workspace(app_state, parts.collect()),
            "show" => Ok(CommandResult::Message(render_permissions_summary(
                app_state,
            ))),
            _ => anyhow::bail!(
                "unknown /permissions action '{}'. Supported: show, mode, allow, deny, ask, trust",
                action
            ),
        }
    }
}

fn trust_workspace(app_state: &AppState, tokens: Vec<&str>) -> anyhow::Result<CommandResult> {
    let path = tokens
        .first()
        .filter(|value| !value.trim().is_empty())
        .map(|value| std::path::PathBuf::from(value.trim()))
        .unwrap_or_else(|| app_state.current_working_directory());
    let permission = tokens
        .get(1)
        .and_then(|value| WorkspacePermissionLevel::parse(value))
        .unwrap_or(WorkspacePermissionLevel::Worker);
    let mut config = load_global_workspace_permissions()
        .unwrap_or_else(|_| WorkspacePermissionConfig::default());
    config.trust_workspace(&path, permission);
    save_global_workspace_permissions(&config)?;
    let config_path = default_workspace_permissions_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "~/.morgo/workspace-permissions.json".into());
    Ok(CommandResult::Message(format!(
        "Trusted workspace {} with {} permission in {}.",
        path.display(),
        permission,
        config_path
    )))
}

#[derive(Clone, Copy)]
enum RuleList {
    Allow,
    Deny,
    Ask,
}

fn update_rule_list(
    app_state: &AppState,
    list: RuleList,
    tokens: Vec<&str>,
) -> anyhow::Result<CommandResult> {
    if tokens.is_empty() {
        anyhow::bail!("usage: /permissions <allow|deny|ask> <rule> [rule...]");
    }

    let mut added = Vec::new();
    for token in tokens {
        let value = token.trim();
        if value.is_empty() {
            continue;
        }
        let inserted = match list {
            RuleList::Allow => app_state.permission_context.add_always_allow_rule(value),
            RuleList::Deny => app_state.permission_context.add_always_deny_rule(value),
            RuleList::Ask => app_state.permission_context.add_always_ask_rule(value),
        };
        if inserted {
            added.push(value.to_string());
        }
    }

    if added.is_empty() {
        return Ok(CommandResult::Message(format!(
            "No new {} rules added.",
            rule_list_name(list)
        )));
    }

    Ok(CommandResult::Message(format!(
        "Added {} rule(s): {}",
        rule_list_name(list),
        added.join(", ")
    )))
}

fn parse_mode(raw: &str) -> anyhow::Result<PermissionMode> {
    match raw {
        "default" => Ok(PermissionMode::Default),
        "plan" => Ok(PermissionMode::Plan),
        "accept-edits" | "accept_edits" => Ok(PermissionMode::AcceptEdits),
        "bypass" | "bypass-permissions" | "bypass_permissions" => {
            Ok(PermissionMode::BypassPermissions)
        }
        _ => anyhow::bail!(
            "unsupported permission mode '{}'; expected default, plan, accept-edits, or bypass",
            raw
        ),
    }
}

fn format_mode(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default => "default",
        PermissionMode::AcceptEdits => "accept-edits",
        PermissionMode::BypassPermissions => "bypass",
        PermissionMode::Plan => "plan",
    }
}

fn rule_list_name(list: RuleList) -> &'static str {
    match list {
        RuleList::Allow => "allow",
        RuleList::Deny => "deny",
        RuleList::Ask => "ask",
    }
}

fn render_permissions_summary(app_state: &AppState) -> String {
    let pending = app_state.permission_context.pending_approval();
    let pending_summary = pending
        .map(|approval| format!("{} — {}", approval.tool_name, approval.message))
        .unwrap_or_else(|| "none".into());

    format!(
        "Permission mode: {}\nWorkspace permission: {}\nAllow rules: {}\nDeny rules: {}\nAsk rules: {}\nPending approval: {}",
        format_mode(app_state.permission_context.mode()),
        format_workspace_permission(app_state),
        format_rules(app_state.permission_context.always_allow_rules()),
        format_rules(app_state.permission_context.always_deny_rules()),
        format_rules(app_state.permission_context.always_ask_rules()),
        pending_summary,
    )
}

fn format_workspace_permission(app_state: &AppState) -> String {
    let cwd = app_state.current_working_directory();
    let Some(config) = app_state.permission_context.workspace_permissions() else {
        return "not loaded".into();
    };
    config
        .effective_permission(&cwd)
        .map(|matched| format!("{} ({})", matched.permission, matched.path.display()))
        .unwrap_or_else(|| "untrusted".into())
}

fn format_rules(rules: Vec<String>) -> String {
    if rules.is_empty() {
        "none".into()
    } else {
        rules.join(", ")
    }
}
