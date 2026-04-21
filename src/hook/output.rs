const MAX_ADDITIONAL_CONTEXT_ENTRIES: usize = 16;
const MAX_ADDITIONAL_CONTEXT_ENTRY_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HookPayload {
    pub updated_input: Option<String>,
    pub additional_context: Vec<String>,
    pub permission_result: HookPermissionResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HookPermissionResult {
    #[default]
    Passthrough,
    Allow {
        updated_input: Option<String>,
        reason: Option<String>,
    },
    // Ask is treated as Passthrough at runtime: the hook signals intent to prompt the user,
    // but the actual approval flow is handled by the permission system, not the hook executor.
    Ask {
        updated_input: Option<String>,
        reason: Option<String>,
    },
    Deny {
        updated_input: Option<String>,
        reason: Option<String>,
    },
}

pub fn sanitize_additional_context(entries: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    entries
        .into_iter()
        .filter_map(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                return None;
            }
            let truncated = if trimmed.len() > MAX_ADDITIONAL_CONTEXT_ENTRY_LEN {
                trimmed[..MAX_ADDITIONAL_CONTEXT_ENTRY_LEN].to_string()
            } else {
                trimmed
            };
            if seen.contains(&truncated) {
                return None;
            }
            seen.insert(truncated.clone());
            Some(truncated)
        })
        .take(MAX_ADDITIONAL_CONTEXT_ENTRIES)
        .collect()
}
