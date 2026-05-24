use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use crate::bootstrap::{InteractionSurface, SessionMode};
use crate::core::boss_state::BossStage;
use crate::state::app_state::RuntimeRole;
use crate::state::permission_context::{BossActorPolicy, ToolPermissionContext};
use crate::tool::definition::{
    InterruptBehavior, ModelToolDefinition, ObservableInput, PermissionDecision, Tool, ToolCall,
    ToolMetadata, ToolResult,
};
use crate::tool::permission::{evaluate_tool_permission, is_tool_allowed};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolRegistrySnapshot {
    pub toolset_id: String,
    #[serde(default)]
    pub visible_tools: Vec<String>,
    #[serde(default)]
    pub allowed_actions: Vec<String>,
    pub schema_hash: String,
    pub permission_hash: String,
    pub actor_role: String,
    pub cwd: PathBuf,
    #[serde(default)]
    pub config_root: Option<PathBuf>,
    #[serde(default)]
    pub workspace_capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolContractMismatch {
    #[serde(default)]
    pub missing_visible_tools: Vec<String>,
    #[serde(default)]
    pub missing_allowed_actions: Vec<String>,
    #[serde(default)]
    pub permission_denied_tools: Vec<String>,
    pub actor_role: String,
    pub cwd: PathBuf,
    #[serde(default)]
    pub config_root: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolContractPreflightSpec {
    pub required_visible_tools: Vec<String>,
    pub required_allowed_actions: Vec<String>,
    pub permission_probe_tools: Vec<String>,
    pub permission_probe_paths: BTreeMap<String, String>,
}

fn one_line_tool_error(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn format_tool_failure(
    tool_name: &str,
    reason: &str,
    message: impl AsRef<str>,
    next_action: &str,
) -> String {
    format!(
        "status=failed\ntool={tool_name}\nreason={reason}\nmessage={}\nnext_action={next_action}\n\nTool was not executed successfully.",
        one_line_tool_error(message.as_ref())
    )
}

fn stable_hash(value: &serde_json::Value) -> String {
    let mut hasher = DefaultHasher::new();
    value.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn actions_for_tool(tool_name: &str) -> &'static [&'static str] {
    match tool_name {
        "Read" => &["read_file"],
        "Edit" => &["edit_file", "write_file"],
        "Write" => &["write_file"],
        "Bash" => &["run_command", "write_file"],
        "LS" | "Glob" => &["list_files"],
        "Grep" => &["search_files"],
        "Agent" => &["spawn_agent"],
        _ => &[],
    }
}

fn sample_call_for_permission_probe(
    tool_name: &str,
    cwd: &Path,
    probe_path: Option<&str>,
) -> ToolCall {
    let default_probe_path = cwd.join("__tool_contract_probe__.txt");
    let file_probe_path = probe_path
        .filter(|path| !path.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| default_probe_path.display().to_string());
    let read_probe_path = probe_path
        .filter(|path| !path.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| cwd.display().to_string());
    match tool_name {
        "Read" => ToolCall::new("Read", json!({ "file_path": read_probe_path }).to_string()),
        "Edit" => ToolCall::new(
            "Edit",
            json!({
                "file_path": file_probe_path,
                "old_string": "before",
                "new_string": "after"
            })
            .to_string(),
        ),
        "Write" => ToolCall::new(
            "Write",
            json!({
                "file_path": file_probe_path,
                "content": "probe"
            })
            .to_string(),
        ),
        "Bash" => ToolCall::new("Bash", json!({ "command": "pwd" }).to_string()),
        "Glob" => ToolCall::new("Glob", json!({ "pattern": "*" }).to_string()),
        "Grep" => ToolCall::new(
            "Grep",
            json!({ "pattern": "mod", "path": cwd.display().to_string() }).to_string(),
        ),
        "LS" => ToolCall::new(
            "LS",
            json!({ "path": cwd.display().to_string() }).to_string(),
        ),
        "Agent" => ToolCall::new("Agent", json!({ "prompt": "permission probe" }).to_string()),
        other => ToolCall::new(other, "{}"),
    }
}

fn render_workspace_capabilities(permissions: &ToolPermissionContext, cwd: &Path) -> Vec<String> {
    let mut capabilities = Vec::new();
    if let Some(config) = permissions.workspace_permissions() {
        if let Some(matched) = config.effective_permission(cwd) {
            capabilities.push(format!(
                "workspace_permission={}",
                matched.permission.as_str()
            ));
            capabilities.push(format!("workspace_scope={}", matched.path.display()));
        } else {
            capabilities.push("workspace_permission=untrusted".into());
        }
    }
    if let Some(config) = permissions.workspace_capability() {
        let effective_tier = config.effective_max_tier(cwd);
        capabilities.push(format!(
            "global_max_tier={}",
            config.global_max_tier.as_str()
        ));
        capabilities.push(format!("effective_max_tier={}", effective_tier.as_str()));
        capabilities.push(format!(
            "escalate_to_pending_approval={}",
            config.escalate_to_pending_approval
        ));
        capabilities.push(format!(
            "audit_capability_decisions={}",
            config.audit_capability_decisions
        ));
    } else {
        capabilities.push("workspace_capability=unset".into());
    }
    capabilities
}

const V1_DEFAULT_CODING_LOCAL_CORE_TOOLS: &[&str] =
    &["Read", "Edit", "Write", "Bash", "Grep", "Glob"];
const V1_DEFAULT_CODING_DEFERRED_TOOLS: &[&str] =
    &["WebSearch", "WebFetch", "AskUserQuestion", "NotebookEdit"];

/// V1 default coding surface currently applies only to the local coordinator path
/// used by CLI/headless coding runs.
fn is_v1_default_coding_surface(context: ToolAssemblyContext) -> bool {
    context.runtime_role == RuntimeRole::Coordinator
        && context.surface == InteractionSurface::Cli
        && context.session_mode == SessionMode::Headless
}

fn is_v1_default_coding_local_core_tool(metadata: &ToolMetadata) -> bool {
    V1_DEFAULT_CODING_LOCAL_CORE_TOOLS.contains(&metadata.name)
}

fn is_v1_default_coding_deferred_tool(metadata: &ToolMetadata) -> bool {
    V1_DEFAULT_CODING_DEFERRED_TOOLS.contains(&metadata.name)
}

fn is_tool_visible_in_v1_default_coding_surface(metadata: &ToolMetadata) -> bool {
    if is_v1_default_coding_local_core_tool(metadata) {
        return true;
    }
    if is_v1_default_coding_deferred_tool(metadata) {
        return false;
    }
    !metadata.is_open_world
}

/// Post-assembly gates should be shared by `visible_tools()` and
/// `visible_model_tools()` so those two views cannot drift.
fn passes_post_assembly_visibility_gates(
    metadata: &ToolMetadata,
    permissions: &ToolPermissionContext,
) -> bool {
    if !(metadata.always_load || (!metadata.should_defer || permissions.include_deferred_tools)) {
        return false;
    }
    if metadata.requires_user_interaction && !permissions.include_interactive_tools {
        return false;
    }
    is_tool_allowed(metadata, permissions)
}

fn passes_surface_assembly_policy(
    metadata: &ToolMetadata,
    context: ToolAssemblyContext,
    permissions: &ToolPermissionContext,
) -> bool {
    if is_v1_default_coding_surface(context) {
        return is_tool_visible_in_v1_default_coding_surface(metadata)
            && is_tool_allowed(metadata, permissions);
    }

    if metadata.is_open_world && !context.include_open_world_tools {
        return false;
    }

    match context.runtime_role {
        RuntimeRole::Coordinator => is_tool_allowed(metadata, permissions),
        RuntimeRole::Worker => {
            if metadata.name == "Agent" || metadata.aliases.contains(&"Agent") {
                // Only ExecutorB in Execution phase may see Agent.
                return context.is_boss_executor_b() && is_tool_allowed(metadata, permissions);
            }
            if metadata.name == "Bash" {
                // Production worker execution needs Bash in headless LisM mode, but
                // actual invocation remains gated later by permission/workspace policy.
                return context.include_open_world_tools;
            }
            is_tool_allowed(metadata, permissions)
        }
    }
}

impl std::fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("tool_count", &self.tools.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAssemblyEnvironment {
    Standard,
    Restricted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolAssemblyContext {
    pub runtime_role: RuntimeRole,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub environment: ToolAssemblyEnvironment,
    pub include_deferred_tools: bool,
    pub include_interactive_tools: bool,
    pub include_open_world_tools: bool,
    /// When set, the Agent tool visibility is governed by boss spawn policy.
    pub boss_actor_policy: Option<BossActorPolicy>,
}

impl ToolAssemblyContext {
    pub fn coordinator(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        let include_open_world_tools = match (surface, session_mode) {
            (InteractionSurface::Cli, SessionMode::Interactive) => true,
            (InteractionSurface::Cli, SessionMode::Print)
            | (InteractionSurface::Cli, SessionMode::InitOnly)
            | (InteractionSurface::Cli, SessionMode::Headless)
            | (InteractionSurface::Remote, _)
            | (InteractionSurface::Telegram, _) => false,
        };
        Self {
            runtime_role: RuntimeRole::Coordinator,
            surface,
            session_mode,
            environment: ToolAssemblyEnvironment::Standard,
            include_deferred_tools: true,
            include_interactive_tools: true,
            include_open_world_tools,
            boss_actor_policy: None,
        }
    }

    pub fn worker(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        Self {
            runtime_role: RuntimeRole::Worker,
            surface,
            session_mode,
            environment: ToolAssemblyEnvironment::Restricted,
            include_deferred_tools: false,
            include_interactive_tools: false,
            include_open_world_tools: true,
            boss_actor_policy: None,
        }
    }

    /// Worker context for ExecutorB in Execution phase — may see a restricted Agent tool.
    pub fn executor_b(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        Self {
            boss_actor_policy: Some(BossActorPolicy::executor_b(BossStage::Execution)),
            include_interactive_tools: true,
            include_open_world_tools: true,
            ..Self::worker(surface, session_mode)
        }
    }

    /// Returns true when this context represents ExecutorB in Execution phase.
    pub fn is_boss_executor_b(&self) -> bool {
        self.boss_actor_policy
            .map(|p| p.may_spawn())
            .unwrap_or(false)
    }

    pub fn permission_context(
        &self,
        mode: crate::state::permission_context::PermissionMode,
    ) -> ToolPermissionContext {
        ToolPermissionContext::new(mode)
            .with_active_surface(self.surface)
            .with_deferred_tools(self.include_deferred_tools)
            .with_interactive_tools(self.include_interactive_tools)
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, tool: Arc<dyn Tool>) -> Self {
        let metadata = tool.metadata();
        assert!(
            self.tools.iter().all(|existing| {
                let existing_metadata = existing.metadata();
                existing_metadata.name != metadata.name
                    && !existing_metadata
                        .aliases
                        .iter()
                        .any(|alias| *alias == metadata.name)
                    && !metadata.aliases.iter().any(|alias| {
                        *alias == existing_metadata.name
                            || existing_metadata
                                .aliases
                                .iter()
                                .any(|existing_alias| existing_alias == alias)
                    })
            }),
            "duplicate or conflicting tool registration: {}",
            metadata.name
        );
        self.tools.push(tool);
        self.tools.sort_by_key(|tool| tool.metadata().name);
        self
    }

    pub fn visible_tools(&self, permissions: &ToolPermissionContext) -> Vec<ToolMetadata> {
        self.tools
            .iter()
            .map(|tool| tool.metadata())
            .filter(|metadata| passes_post_assembly_visibility_gates(metadata, permissions))
            .collect()
    }

    pub fn visible_model_tools(
        &self,
        permissions: &ToolPermissionContext,
    ) -> Vec<ModelToolDefinition> {
        self.tools
            .iter()
            .filter_map(|tool| {
                let metadata = tool.metadata();
                if !passes_post_assembly_visibility_gates(&metadata, permissions) {
                    return None;
                }
                let input_schema = tool.input_schema()?;
                Some(ModelToolDefinition {
                    name: metadata.name.to_string(),
                    description: metadata.description.to_string(),
                    input_schema,
                })
            })
            .collect()
    }

    pub async fn derive_allowed_actions(
        &self,
        permissions: &ToolPermissionContext,
        cwd: &Path,
    ) -> Vec<String> {
        let mut actions = BTreeSet::new();
        for tool_name in self.visible_tool_names(permissions) {
            if !self
                .is_tool_invokable(tool_name.as_str(), permissions, cwd, None)
                .await
            {
                continue;
            }
            for action in actions_for_tool(tool_name.as_str()) {
                actions.insert(action.to_string());
            }
        }
        actions.into_iter().collect()
    }

    pub fn visible_tool_names(&self, permissions: &ToolPermissionContext) -> Vec<String> {
        self.visible_tools(permissions)
            .into_iter()
            .map(|metadata| metadata.name.to_string())
            .collect()
    }

    pub async fn snapshot(
        &self,
        permissions: &ToolPermissionContext,
        toolset_id: impl Into<String>,
        actor_role: impl Into<String>,
        cwd: PathBuf,
        config_root: Option<PathBuf>,
    ) -> ToolRegistrySnapshot {
        let visible_tool_metadata = self.visible_tools(permissions);
        let visible_model_tools = self.visible_model_tools(permissions);
        let visible_tools = visible_tool_metadata
            .iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>();
        let allowed_actions = self.derive_allowed_actions(permissions, &cwd).await;
        let schema_hash = stable_hash(&json!(
            visible_tool_metadata
                .iter()
                .map(|tool| json!({
                    "name": tool.name,
                    "input_schema": visible_model_tools
                        .iter()
                        .find(|model_tool| model_tool.name == tool.name)
                        .map(|model_tool| model_tool.input_schema.clone()),
                }))
                .collect::<Vec<_>>()
        ));
        let workspace_capabilities = render_workspace_capabilities(permissions, &cwd);
        let permission_hash = stable_hash(&json!({
            "mode": format!("{:?}", permissions.mode()),
            "always_allow_rules": permissions.always_allow_rules(),
            "always_ask_rules": permissions.always_ask_rules(),
            "always_deny_rules": permissions.always_deny_rules(),
            "include_deferred_tools": permissions.include_deferred_tools,
            "include_interactive_tools": permissions.include_interactive_tools,
            "active_surface": permissions.active_surface.map(|surface| format!("{surface:?}")),
            "boss_actor_policy": permissions.boss_actor_policy.map(|policy| json!({
                "actor_role": format!("{:?}", policy.actor_role),
                "lineage_depth": policy.lineage_depth,
                "phase": format!("{:?}", policy.phase),
            })),
            "workspace_capabilities": workspace_capabilities,
        }));
        ToolRegistrySnapshot {
            toolset_id: toolset_id.into(),
            visible_tools,
            allowed_actions,
            schema_hash,
            permission_hash,
            actor_role: actor_role.into(),
            cwd,
            config_root,
            workspace_capabilities,
        }
    }

    pub async fn preflight_contract(
        &self,
        permissions: &ToolPermissionContext,
        snapshot: &ToolRegistrySnapshot,
        spec: &ToolContractPreflightSpec,
    ) -> Result<(), ToolContractMismatch> {
        let missing_visible_tools = spec
            .required_visible_tools
            .iter()
            .filter(|tool| {
                !snapshot
                    .visible_tools
                    .iter()
                    .any(|visible| visible == *tool)
            })
            .cloned()
            .collect::<Vec<_>>();
        let missing_allowed_actions = spec
            .required_allowed_actions
            .iter()
            .filter(|action| {
                !snapshot
                    .allowed_actions
                    .iter()
                    .any(|allowed| allowed == *action)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut permission_denied_tools = Vec::new();
        for tool_name in &spec.permission_probe_tools {
            if !snapshot
                .visible_tools
                .iter()
                .any(|visible| visible == tool_name)
            {
                continue;
            }
            let probe_path = spec
                .permission_probe_paths
                .get(tool_name)
                .map(|path| path.as_str());
            if !self
                .is_tool_invokable(tool_name.as_str(), permissions, &snapshot.cwd, probe_path)
                .await
            {
                permission_denied_tools.push(tool_name.clone());
            }
        }
        if missing_visible_tools.is_empty()
            && missing_allowed_actions.is_empty()
            && permission_denied_tools.is_empty()
        {
            return Ok(());
        }
        Err(ToolContractMismatch {
            missing_visible_tools,
            missing_allowed_actions,
            permission_denied_tools,
            actor_role: snapshot.actor_role.clone(),
            cwd: snapshot.cwd.clone(),
            config_root: snapshot.config_root.clone(),
        })
    }

    pub fn all_metadata(&self) -> Vec<ToolMetadata> {
        self.tools.iter().map(|tool| tool.metadata()).collect()
    }

    pub fn assemble(&self, context: ToolAssemblyContext) -> Self {
        let permissions =
            context.permission_context(crate::state::permission_context::PermissionMode::Default);
        let tools = self
            .tools
            .iter()
            .filter(|tool| passes_surface_assembly_policy(&tool.metadata(), context, &permissions))
            .cloned()
            .collect();
        Self { tools }
    }

    pub fn assemble_for_role(&self, role: RuntimeRole) -> Self {
        let context = match role {
            RuntimeRole::Coordinator => {
                ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Interactive)
            }
            RuntimeRole::Worker => {
                ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless)
            }
        };
        self.assemble(context)
    }

    pub fn filter_for_worker(&self) -> Self {
        self.assemble_for_role(RuntimeRole::Worker)
    }

    pub fn assemble_worker_registry(&self, allowed_tools: Option<&[String]>) -> Self {
        let worker = self.assemble_for_role(RuntimeRole::Worker);
        let Some(allowed_tools) = allowed_tools else {
            return worker;
        };
        let tools = worker
            .tools
            .iter()
            .filter(|tool| {
                let metadata = tool.metadata();
                allowed_tools.iter().any(|allowed| {
                    allowed == metadata.name
                        || metadata.aliases.iter().any(|alias| allowed == alias)
                })
            })
            .cloned()
            .collect();
        Self { tools }
    }

    pub fn find(&self, call: &ToolCall) -> Option<&Arc<dyn Tool>> {
        self.tools.iter().find(|tool| {
            let metadata = tool.metadata();
            metadata.name == call.name || metadata.aliases.iter().any(|alias| *alias == call.name)
        })
    }

    pub fn is_concurrency_safe(&self, call: &ToolCall) -> Option<bool> {
        self.find(call).map(|tool| tool.is_concurrency_safe(call))
    }

    pub fn interrupt_behavior(&self, call: &ToolCall) -> Option<InterruptBehavior> {
        self.find(call).map(|tool| tool.interrupt_behavior())
    }

    pub fn observable_input(&self, call: &ToolCall) -> Option<ObservableInput> {
        self.find(call).and_then(|tool| {
            tool.backfill_observable_input(call)
                .or_else(|| tool.observable_input(call))
        })
    }

    pub async fn permission_decision(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> Option<PermissionDecision> {
        let tool = self.find(call)?;
        let metadata = tool.metadata();
        let base_decision = evaluate_tool_permission(&metadata, call, permissions);
        let tool_decision = tool.check_permissions(call, permissions).await;
        Some(merge_permission_decisions(base_decision, tool_decision))
    }

    pub async fn is_tool_invokable(
        &self,
        tool_name: &str,
        permissions: &ToolPermissionContext,
        cwd: &Path,
        probe_path: Option<&str>,
    ) -> bool {
        let call = sample_call_for_permission_probe(tool_name, cwd, probe_path);
        matches!(
            self.permission_decision(&call, permissions).await,
            Some(PermissionDecision::Allow)
        )
    }

    pub async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.find(call) else {
            return Ok(ToolResult::Text(format_tool_failure(
                &call.name,
                "unknown_tool",
                format!("unknown tool {}", call.name),
                "Use one of the available tools from the current tool list.",
            )));
        };

        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            return Ok(ToolResult::Text(format_tool_failure(
                metadata.name,
                "schema_invalid",
                format!("tool {} requires JSON-structured input", metadata.name),
                "Retry the tool call with valid JSON matching the tool schema.",
            )));
        }
        if let Err(error) = tool.validate_input(call).await {
            return Ok(ToolResult::Text(format_tool_failure(
                metadata.name,
                "input_invalid",
                format!("invalid input for {}: {}", metadata.name, error),
                "Retry the tool call with corrected arguments.",
            )));
        }
        let base_decision = evaluate_tool_permission(&metadata, call, permissions);
        let tool_decision = tool.check_permissions(call, permissions).await;
        let resolved_decision = merge_permission_decisions(base_decision, tool_decision);
        match resolved_decision {
            crate::tool::definition::PermissionDecision::Allow => {
                match tool.invoke(call, permissions).await {
                    Ok(result) => Ok(result),
                    Err(error) => Ok(ToolResult::Text(format_tool_failure(
                        metadata.name,
                        "tool_error",
                        error.to_string(),
                        "Read the error message, correct the tool arguments or environment assumptions, and retry if appropriate.",
                    ))),
                }
            }
            crate::tool::definition::PermissionDecision::Ask {
                message,
                metadata: approval_metadata,
                ..
            } => {
                let approval = if let Some(approval_metadata) = approval_metadata {
                    crate::tool::result::PendingApprovalPayload {
                        code: approval_metadata.code,
                        summary: approval_metadata
                            .summary
                            .unwrap_or_else(|| format!("{} pending approval", metadata.name)),
                        detail: approval_metadata.detail.or_else(|| Some(message.clone())),
                        approval_kind: approval_metadata
                            .approval_kind
                            .or_else(|| Some("tool_permission".into())),
                        escalation_reasons: approval_metadata.escalation_reasons,
                    }
                } else {
                    crate::tool::result::PendingApprovalPayload {
                        code: None,
                        summary: format!("{} pending approval", metadata.name),
                        detail: Some(message.clone()),
                        approval_kind: Some("tool_permission".into()),
                        escalation_reasons: Vec::new(),
                    }
                };
                Ok(ToolResult::PendingApproval {
                    tool_name: metadata.name.to_string(),
                    approval,
                    message,
                })
            }
            crate::tool::definition::PermissionDecision::Deny { message, .. } => {
                Ok(ToolResult::Denied(message))
            }
        }
    }

    pub async fn invoke_with_approval(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.find(call) else {
            return Ok(ToolResult::Text(format_tool_failure(
                &call.name,
                "unknown_tool",
                format!("unknown tool {}", call.name),
                "Use one of the available tools from the current tool list.",
            )));
        };
        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            return Ok(ToolResult::Text(format_tool_failure(
                metadata.name,
                "schema_invalid",
                format!("tool {} requires JSON-structured input", metadata.name),
                "Retry the tool call with valid JSON matching the tool schema.",
            )));
        }
        if let Err(error) = tool.validate_input(call).await {
            return Ok(ToolResult::Text(format_tool_failure(
                metadata.name,
                "input_invalid",
                format!("invalid input for {}: {}", metadata.name, error),
                "Retry the tool call with corrected arguments.",
            )));
        }
        match tool.invoke(call, permissions).await {
            Ok(result) => Ok(result),
            Err(error) => Ok(ToolResult::Text(format_tool_failure(
                metadata.name,
                "tool_error",
                error.to_string(),
                "Read the error message, correct the tool arguments or environment assumptions, and retry if appropriate.",
            ))),
        }
    }
}

fn merge_permission_decisions(
    base: crate::tool::definition::PermissionDecision,
    tool: crate::tool::definition::PermissionDecision,
) -> crate::tool::definition::PermissionDecision {
    use crate::tool::definition::PermissionDecision::{Allow, Ask, Deny};

    match (base, tool) {
        (Deny { message, reason }, _) | (_, Deny { message, reason }) => Deny { message, reason },
        (
            Ask {
                message,
                reason,
                metadata,
            },
            _,
        ) => Ask {
            message,
            reason,
            metadata,
        },
        (
            _,
            Ask {
                message,
                reason,
                metadata,
            },
        ) => Ask {
            message,
            reason,
            metadata,
        },
        (Allow, Allow) => Allow,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::permission_context::PermissionMode;
    use async_trait::async_trait;

    struct FailingTool {
        validate_error: bool,
    }

    #[async_trait]
    impl Tool for FailingTool {
        fn metadata(&self) -> ToolMetadata {
            ToolMetadata {
                name: "FailingTool",
                description: "test tool",
                aliases: &[],
                search_hint: None,
                read_only: true,
                destructive: false,
                concurrency_safe: true,
                always_load: true,
                should_defer: false,
                requires_auth: false,
                requires_user_interaction: false,
                is_open_world: false,
                is_search_or_read_command: false,
            }
        }

        async fn validate_input(&self, _call: &ToolCall) -> anyhow::Result<()> {
            if self.validate_error {
                anyhow::bail!("bad test input");
            }
            Ok(())
        }

        async fn invoke(
            &self,
            _call: &ToolCall,
            _permissions: &ToolPermissionContext,
        ) -> anyhow::Result<ToolResult> {
            anyhow::bail!("runtime test failure")
        }
    }

    #[tokio::test]
    async fn unknown_tool_returns_model_visible_failure_text() {
        let result = ToolRegistry::new()
            .invoke(
                &ToolCall::new("MissingTool", "{}"),
                &ToolPermissionContext::new(PermissionMode::Default),
            )
            .await
            .expect("registry should return a tool result");

        let ToolResult::Text(text) = result else {
            panic!("expected model-visible text failure");
        };
        assert!(text.contains("status=failed"));
        assert!(text.contains("reason=unknown_tool"));
        assert!(text.contains("unknown tool MissingTool"));
    }

    #[tokio::test]
    async fn validation_error_returns_model_visible_failure_text() {
        let registry = ToolRegistry::new().register(Arc::new(FailingTool {
            validate_error: true,
        }));
        let result = registry
            .invoke(
                &ToolCall::new("FailingTool", "{}"),
                &ToolPermissionContext::new(PermissionMode::Default),
            )
            .await
            .expect("registry should return a tool result");

        let ToolResult::Text(text) = result else {
            panic!("expected model-visible text failure");
        };
        assert!(text.contains("status=failed"));
        assert!(text.contains("reason=input_invalid"));
        assert!(text.contains("bad test input"));
    }

    #[tokio::test]
    async fn invoke_error_returns_model_visible_failure_text() {
        let registry = ToolRegistry::new().register(Arc::new(FailingTool {
            validate_error: false,
        }));
        let result = registry
            .invoke(
                &ToolCall::new("FailingTool", "{}"),
                &ToolPermissionContext::new(PermissionMode::Default),
            )
            .await
            .expect("registry should return a tool result");

        let ToolResult::Text(text) = result else {
            panic!("expected model-visible text failure");
        };
        assert!(text.contains("status=failed"));
        assert!(text.contains("reason=tool_error"));
        assert!(text.contains("runtime test failure"));
    }
}
