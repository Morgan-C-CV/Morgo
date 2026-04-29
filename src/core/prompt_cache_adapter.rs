use serde_json::{Value, json};

use crate::core::prompt_budget::{PromptCacheCapability, ProviderProfile};
use crate::core::prompt_segment::PromptAssembly;

/// Inject Anthropic ephemeral cache-control blocks into an already-constructed payload.
///
/// Cacheable segments → `system` array; the last cacheable block gets
/// `"cache_control": { "type": "ephemeral" }`.
/// Dynamic segments → `messages[0].content` array (replaces the existing entry).
/// If there are no cacheable segments the payload is left unchanged.
pub fn apply_anthropic_cache_control(assembly: &PromptAssembly, payload: &mut Value) {
    let cacheable: Vec<_> = assembly
        .segments()
        .iter()
        .filter(|s| s.is_cacheable())
        .collect();
    if cacheable.is_empty() {
        return;
    }

    let last_idx = cacheable.len() - 1;
    let system_blocks: Vec<Value> = cacheable
        .iter()
        .enumerate()
        .map(|(i, seg)| {
            let mut block = json!({ "type": "text", "text": seg.content });
            if i == last_idx {
                block["cache_control"] = json!({ "type": "ephemeral" });
            }
            block
        })
        .collect();
    payload["system"] = json!(system_blocks);

    let dynamic: Vec<Value> = assembly
        .segments()
        .iter()
        .filter(|s| !s.is_cacheable())
        .map(|s| json!({ "type": "text", "text": s.content }))
        .collect();
    if !dynamic.is_empty() {
        payload["messages"] = json!([{ "role": "user", "content": dynamic }]);
    }
}

/// Post-process a provider request payload to inject cache-control annotations
/// based on the provider's declared cache capability.
///
/// Pure function — does not modify `assembly` or `profile`.
/// `Unsupported`, `ManualNone`, and `OpenAICompatiblePrefix` are no-ops in v1.
pub fn apply_cache_control(
    assembly: &PromptAssembly,
    profile: &ProviderProfile,
    payload: &mut Value,
) {
    match profile.prompt_cache {
        PromptCacheCapability::AnthropicEphemeral => {
            apply_anthropic_cache_control(assembly, payload)
        }
        PromptCacheCapability::Unsupported
        | PromptCacheCapability::ManualNone
        | PromptCacheCapability::OpenAICompatiblePrefix => {}
    }
}
