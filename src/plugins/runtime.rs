use std::future::pending;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use tokio::time::sleep;
use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc};

use crate::hook::registry::HookRegistry;
use crate::plugins::loader::validate_runtime_artifact_canonicalized;
use crate::plugins::types::{
    PluginDiagnostic, PluginDiagnosticSeverity, PluginLoadResult, PluginRuntimeKind,
    PluginRuntimeSpec, PluginToolDefinition,
};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

const DEFAULT_RUNTIME_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_RUNTIME_OUTPUT_CAP_BYTES: u64 = 65_536;
const DEFAULT_RUNTIME_ENTRY: &str = "run_tool";
const ALLOC_INPUT_EXPORT: &str = "alloc_input";

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
                Some(PluginRuntimeKind::Wasm) => PluginRuntimeTool::new(
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
    artifact_path: PathBuf,
    artifact_summary: String,
    entry: String,
    timeout_ms: u64,
    output_cap_bytes: u64,
    capability_summary: String,
}

struct PluginRuntimeInvocationMetadata {
    plugin_name: String,
    tool_name: String,
    runtime_kind: PluginRuntimeKind,
    artifact_summary: String,
    entry: String,
    timeout_ms: u64,
    output_cap_bytes: u64,
    capability_summary: String,
    duration_ms: u128,
    timeout_hit: bool,
    output_cap_hit: bool,
    final_result_kind: &'static str,
}

struct PluginRuntimeTool {
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

impl PluginRuntimeTool {
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
                artifact_path: canonical_artifact,
                entry: runtime
                    .entry
                    .clone()
                    .unwrap_or_else(|| DEFAULT_RUNTIME_ENTRY.into()),
                timeout_ms: runtime.timeout_ms.unwrap_or(DEFAULT_RUNTIME_TIMEOUT_MS),
                output_cap_bytes: runtime
                    .output_cap_bytes
                    .unwrap_or(DEFAULT_RUNTIME_OUTPUT_CAP_BYTES),
                capability_summary: render_runtime_capability_summary(runtime),
            },
        })
    }
}

impl PluginRuntimeToolMetadata {
    fn begin_invocation(&self) -> PluginRuntimeInvocationMetadata {
        PluginRuntimeInvocationMetadata {
            plugin_name: self.plugin_name.clone(),
            tool_name: self.tool_name.clone(),
            runtime_kind: self.runtime_kind,
            artifact_summary: self.artifact_summary.clone(),
            entry: self.entry.clone(),
            timeout_ms: self.timeout_ms,
            output_cap_bytes: self.output_cap_bytes,
            capability_summary: self.capability_summary.clone(),
            duration_ms: 0,
            timeout_hit: false,
            output_cap_hit: false,
            final_result_kind: "unknown",
        }
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

fn finalize_invocation_metadata(
    metadata: &mut PluginRuntimeInvocationMetadata,
    started_at: Instant,
    result: &ToolResult,
) {
    metadata.duration_ms = started_at.elapsed().as_millis();
    match result {
        ToolResult::Text(_) => metadata.final_result_kind = "text",
        ToolResult::Denied(_) => metadata.final_result_kind = "denied",
        ToolResult::PendingApproval { .. } => metadata.final_result_kind = "pending_approval",
        ToolResult::Interrupted(_) => {
            metadata.final_result_kind = "interrupted";
            metadata.timeout_hit = true;
        }
        ToolResult::Progress(_) => metadata.final_result_kind = "progress",
        ToolResult::ResultTooLarge(_) => {
            metadata.final_result_kind = "result_too_large";
            metadata.output_cap_hit = true;
        }
    }
}

fn render_runtime_message(
    header: &str,
    metadata: &PluginRuntimeInvocationMetadata,
    detail: Option<&str>,
) -> String {
    let mut message = format!(
        "{header}\nPlugin: {}\nTool: {}\nRuntime: {}\nArtifact: {}\nEntry: {}\nTimeout ms: {}\nOutput cap bytes: {}\nCapabilities: {}\nDuration ms: {}\nTimeout hit: {}\nOutput cap hit: {}\nResult: {}",
        metadata.plugin_name,
        metadata.tool_name,
        metadata.runtime_kind.as_str(),
        metadata.artifact_summary,
        metadata.entry,
        metadata.timeout_ms,
        metadata.output_cap_bytes,
        metadata.capability_summary,
        metadata.duration_ms,
        if metadata.timeout_hit { "yes" } else { "no" },
        if metadata.output_cap_hit { "yes" } else { "no" },
        metadata.final_result_kind,
    );
    if let Some(detail) = detail {
        message.push('\n');
        message.push_str(detail);
    }
    message
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
impl Tool for PluginRuntimeTool {
    fn metadata(&self) -> ToolMetadata {
        self.metadata.clone()
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let started_at = Instant::now();
        let mut invocation = self.runtime_metadata.begin_invocation();
        let timeout_hit = Arc::new(AtomicBool::new(false));
        let cancellation_hit = Arc::new(AtomicBool::new(false));
        let engine = build_wasm_engine()?;
        let engine_for_interrupt = engine.clone();
        let timeout_hit_for_interrupt = timeout_hit.clone();
        let cancellation_hit_for_interrupt = cancellation_hit.clone();
        let cancellation_token = permissions.cancellation_token.clone();
        let timeout_ms = self.runtime_metadata.timeout_ms;
        let interrupt_task = tokio::spawn(async move {
            tokio::select! {
                _ = sleep(Duration::from_millis(timeout_ms)) => {
                    timeout_hit_for_interrupt.store(true, Ordering::Release);
                    engine_for_interrupt.increment_epoch();
                }
                _ = async {
                    if let Some(token) = cancellation_token {
                        token.cancelled().await;
                    } else {
                        pending::<()>().await;
                    }
                } => {
                    cancellation_hit_for_interrupt.store(true, Ordering::Release);
                    engine_for_interrupt.increment_epoch();
                }
            }
        });

        let artifact_path = self.runtime_metadata.artifact_path.clone();
        let entry = self.runtime_metadata.entry.clone();
        let output_cap_bytes = self.runtime_metadata.output_cap_bytes;
        let input = call.raw_input().as_bytes().to_vec();
        let execution = tokio::task::spawn_blocking(move || {
            execute_wasm_tool_call(&engine, &artifact_path, &entry, &input, output_cap_bytes)
        })
        .await;

        interrupt_task.abort();

        let tool_result = match execution {
            Ok(Ok(output)) => ToolResult::Text(output),
            Ok(Err(error)) if is_epoch_interruption(&error) => {
                invocation.timeout_hit =
                    timeout_hit.load(Ordering::Acquire) || cancellation_hit.load(Ordering::Acquire);
                ToolResult::Interrupted(render_runtime_message(
                    "plugin runtime execution interrupted",
                    &invocation,
                    Some(&error.to_string()),
                ))
            }
            Ok(Err(error)) if is_output_cap_error(&error) => {
                ToolResult::ResultTooLarge(render_runtime_message(
                    "plugin runtime result exceeded output cap",
                    &invocation,
                    Some(&error.to_string()),
                ))
            }
            Ok(Err(error)) if is_missing_alloc_input_error(&error) => {
                ToolResult::Denied(render_runtime_message(
                    "plugin runtime alloc_input export is required",
                    &invocation,
                    Some(&error.to_string()),
                ))
            }
            Ok(Err(error)) if is_missing_import_error(&error) => {
                ToolResult::Denied(render_runtime_message(
                    "plugin runtime host imports are not available",
                    &invocation,
                    Some(&error.to_string()),
                ))
            }
            Ok(Err(error)) => return Err(error),
            Err(error) => {
                if is_epoch_join_error(&error) {
                    invocation.timeout_hit = timeout_hit.load(Ordering::Acquire)
                        || cancellation_hit.load(Ordering::Acquire);
                    ToolResult::Interrupted(render_runtime_message(
                        "plugin runtime execution interrupted",
                        &invocation,
                        Some(&error.to_string()),
                    ))
                } else {
                    return Err(anyhow::Error::new(error).context("plugin runtime task failed"));
                }
            }
        };

        finalize_invocation_metadata(&mut invocation, started_at, &tool_result);
        Ok(match tool_result {
            ToolResult::Text(output) => ToolResult::Text(output),
            ToolResult::Denied(message) => ToolResult::Denied(message),
            ToolResult::Interrupted(_) => ToolResult::Interrupted(render_runtime_message(
                "plugin runtime execution interrupted",
                &invocation,
                None,
            )),
            ToolResult::ResultTooLarge(_) => ToolResult::ResultTooLarge(render_runtime_message(
                "plugin runtime result exceeded output cap",
                &invocation,
                None,
            )),
            other => other,
        })
    }
}

fn build_wasm_engine() -> anyhow::Result<Engine> {
    let mut config = Config::new();
    config.epoch_interruption(true);
    Engine::new(&config).context("failed to create wasmtime engine")
}

fn execute_wasm_tool_call(
    engine: &Engine,
    artifact_path: &Path,
    entry: &str,
    input: &[u8],
    output_cap_bytes: u64,
) -> anyhow::Result<String> {
    let module = Module::from_file(engine, artifact_path)
        .with_context(|| format!("failed to load wasm module {}", artifact_path.display()))?;
    let mut store = Store::new(engine, ());
    store.set_epoch_deadline(1);
    let instance = Instance::new(&mut store, &module, &[]).with_context(|| {
        format!(
            "failed to instantiate wasm module {}",
            artifact_path.display()
        )
    })?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("wasm module must export memory"))?;
    let alloc_input: TypedFunc<i32, i32> = instance
        .get_typed_func(&mut store, ALLOC_INPUT_EXPORT)
        .with_context(|| format!("failed to resolve wasm export {ALLOC_INPUT_EXPORT}"))?;
    let run_tool: TypedFunc<(i32, i32), i64> = instance
        .get_typed_func(&mut store, entry)
        .with_context(|| format!("failed to resolve wasm export {entry}"))?;

    let input_ptr = alloc_input
        .call(&mut store, input.len() as i32)
        .context("wasm input allocation failed")?;
    if input_ptr < 0 {
        anyhow::bail!("wasm alloc_input returned negative pointer: {input_ptr}");
    }

    memory
        .write(&mut store, input_ptr as usize, input)
        .context("failed to write tool input into wasm memory")?;

    let packed = run_tool
        .call(&mut store, (input_ptr, input.len() as i32))
        .context("wasm tool execution failed")?;
    let output_ptr = (packed & 0xffff_ffff) as usize;
    let output_len = ((packed >> 32) & 0xffff_ffff) as usize;

    if output_len as u64 > output_cap_bytes {
        anyhow::bail!("plugin runtime output exceeded cap: {output_len} > {output_cap_bytes}");
    }

    let mut output = vec![0; output_len];
    memory
        .read(&store, output_ptr, &mut output)
        .context("failed to read wasm output from memory")?;
    String::from_utf8(output).context("wasm output must be valid utf-8")
}

fn is_epoch_interruption(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("interrupt")
            || message.contains("epoch deadline")
            || message.contains("wasm trap: interrupt")
    })
}

fn is_epoch_join_error(error: &tokio::task::JoinError) -> bool {
    error.is_panic() && error.to_string().contains("wasm trap: interrupt")
}

fn is_output_cap_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("plugin runtime output exceeded cap")
}

fn is_missing_alloc_input_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("failed to resolve wasm export alloc_input")
}

fn is_missing_import_error(error: &anyhow::Error) -> bool {
    error.to_string().contains("expected import") || error.to_string().contains("unknown import")
}
