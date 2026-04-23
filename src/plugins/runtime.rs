use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::hook::registry::HookRegistry;
use crate::plugins::loader::validate_runtime_artifact_canonicalized;
use crate::plugins::types::{
    PluginDiagnostic, PluginDiagnosticSeverity, PluginLoadResult, PluginRuntimeKind,
    PluginRuntimeSpec, PluginToolDefinition,
};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

pub fn augment_hook_registry_with_plugins(
    mut registry: HookRegistry,
    plugin_load_result: &PluginLoadResult,
) -> HookRegistry {
    for hook in plugin_load_result
        .plugins
        .iter()
        .flat_map(|plugin| plugin.active_hooks().into_iter())
    {
        registry = registry.register_rule(hook.to_rule());
    }
    registry
}

pub fn augment_tool_registry_with_plugins(
    mut registry: ToolRegistry,
    plugin_load_result: &PluginLoadResult,
) -> (ToolRegistry, Vec<PluginDiagnostic>) {
    let mut diagnostics = Vec::new();

    for plugin in &plugin_load_result.plugins {
        let runtime = plugin.runtime.as_ref();
        for tool in plugin.active_tools() {
            let registration = match runtime.map(|runtime| runtime.kind) {
                Some(PluginRuntimeKind::Wasm) => PluginRuntimeToolPlaceholder::new(
                    tool.clone(),
                    runtime.expect("runtime.kind=wasm implies runtime exists"),
                )
                .map(|tool| Arc::new(tool) as Arc<dyn Tool>),
                _ => {
                    PluginPromptTool::new(tool.clone()).map(|tool| Arc::new(tool) as Arc<dyn Tool>)
                }
            };
            match registration {
                Ok(tool) => {
                    registry = registry.register(tool);
                }
                Err(message) => diagnostics.push(PluginDiagnostic {
                    plugin_name: Some(tool.plugin_name.clone()),
                    manifest_path: Some(tool.manifest_path.clone()),
                    severity: PluginDiagnosticSeverity::Error,
                    code: "plugin-tool-registration-failed".into(),
                    message: message.to_string(),
                }),
            }
        }
    }

    (registry, diagnostics)
}

struct PluginPromptTool {
    metadata: ToolMetadata,
    plugin_name: String,
    prompt: String,
    manifest_path: String,
}

struct PluginRuntimeToolMetadata {
    plugin_name: String,
    tool_name: String,
    runtime_kind: PluginRuntimeKind,
    artifact_summary: String,
    timeout_ms: Option<u64>,
    output_cap_bytes: Option<u64>,
    capability_summary: String,
}

struct PluginRuntimeToolPlaceholder {
    metadata: ToolMetadata,
    runtime_metadata: PluginRuntimeToolMetadata,
}

impl PluginPromptTool {
    fn new(definition: PluginToolDefinition) -> anyhow::Result<Self> {
        let display_name = definition.name.clone();
        Ok(Self {
            metadata: build_tool_metadata(&definition),
            plugin_name: format!("{} ({display_name})", definition.plugin_name),
            prompt: definition.prompt,
            manifest_path: definition.manifest_path.display().to_string(),
        })
    }
}

impl PluginRuntimeToolPlaceholder {
    fn new(definition: PluginToolDefinition, runtime: &PluginRuntimeSpec) -> anyhow::Result<Self> {
        let artifact = runtime
            .artifact
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("runtime.kind=wasm requires runtime.artifact"))?;
        let manifest_dir = definition
            .manifest_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let canonical_artifact = validate_runtime_artifact_canonicalized(
            &definition.plugin_name,
            &definition.manifest_path,
            manifest_dir,
            artifact,
        )
        .map_err(|diagnostic| anyhow::anyhow!(diagnostic.render_line()))?;
        Ok(Self {
            metadata: build_tool_metadata(&definition),
            runtime_metadata: PluginRuntimeToolMetadata {
                plugin_name: definition.plugin_name,
                tool_name: definition.name,
                runtime_kind: runtime.kind,
                artifact_summary: canonical_artifact.display().to_string(),
                timeout_ms: runtime.timeout_ms,
                output_cap_bytes: runtime.output_cap_bytes,
                capability_summary: render_runtime_capability_summary(runtime),
            },
        })
    }
}

fn build_tool_metadata(definition: &PluginToolDefinition) -> ToolMetadata {
    let qualified_name = definition.qualified_tool_name();
    let description: &'static str = Box::leak(definition.description.clone().into_boxed_str());
    let aliases: &'static [&'static str] = Box::leak(
        definition
            .aliases
            .iter()
            .cloned()
            .map(|alias| Box::leak(alias.into_boxed_str()) as &'static str)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let search_hint = definition
        .search_hint
        .clone()
        .map(|hint| Box::leak(hint.into_boxed_str()) as &'static str);
    ToolMetadata {
        name: Box::leak(qualified_name.into_boxed_str()),
        description,
        aliases,
        search_hint,
        read_only: definition.read_only,
        destructive: definition.destructive,
        concurrency_safe: true,
        always_load: true,
        should_defer: false,
        requires_auth: definition.requires_auth,
        requires_user_interaction: definition.requires_user_interaction,
        is_open_world: false,
        is_search_or_read_command: definition.read_only,
    }
}

fn render_runtime_capability_summary(runtime: &PluginRuntimeSpec) -> String {
    let Some(capabilities) = runtime.capabilities.as_ref() else {
        return "none".into();
    };

    let mut parts = Vec::new();
    if let Some(filesystem) = capabilities.filesystem.as_ref() {
        parts.push(format!(
            "filesystem(read_roots=[{}], write_roots=[{}])",
            filesystem.read_roots.join(","),
            filesystem.write_roots.join(",")
        ));
    }
    if let Some(network) = capabilities.network.as_ref() {
        parts.push(format!(
            "network(allow_hosts=[{}])",
            network.allow_hosts.join(",")
        ));
    }
    if let Some(env) = capabilities.env.as_ref() {
        parts.push(format!("env(allow_names=[{}])", env.allow_names.join(",")));
    }

    if parts.is_empty() {
        "none".into()
    } else {
        parts.join("; ")
    }
}

#[async_trait]
impl Tool for PluginPromptTool {
    fn metadata(&self) -> ToolMetadata {
        self.metadata.clone()
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let args = call.raw_input().trim();
        let args_line = if args.is_empty() {
            "Arguments: (none)".to_string()
        } else {
            format!("Arguments: {args}")
        };
        Ok(ToolResult::Text(format!(
            "Loaded plugin tool: {}\nPlugin: {}\n{}\nManifest: {}\n\nPlugin tool instructions:\n{}",
            self.metadata.name, self.plugin_name, args_line, self.manifest_path, self.prompt
        )))
    }
}

#[async_trait]
impl Tool for PluginRuntimeToolPlaceholder {
    fn metadata(&self) -> ToolMetadata {
        self.metadata.clone()
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Denied(format!(
            "plugin runtime execution is not enabled\nPlugin: {}\nTool: {}\nRuntime: {}\nArtifact: {}\nTimeout ms: {}\nOutput cap bytes: {}\nCapabilities: {}",
            self.runtime_metadata.plugin_name,
            self.runtime_metadata.tool_name,
            self.runtime_metadata.runtime_kind.as_str(),
            self.runtime_metadata.artifact_summary,
            self.runtime_metadata
                .timeout_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".into()),
            self.runtime_metadata
                .output_cap_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".into()),
            self.runtime_metadata.capability_summary,
        )))
    }
}
