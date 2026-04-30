#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactPlanKind {
    AutoCompact,
    ReactiveCompact,
    CollapseDrain,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactPlan {
    pub kind: CompactPlanKind,
    pub notice_kind: &'static str,
    pub notice_message: String,
    pub assistant_message: Option<String>,
    pub retry_prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactRecoveryErrorContext<'a> {
    pub kind: &'a str,
    pub message: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactServiceNextStep {
    RetryReactiveCompact,
    RetryCollapseDrain,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactServiceResult {
    pub plan: CompactPlan,
    pub next_step: CompactServiceNextStep,
    pub tracking_key: &'static str,
    pub should_record_observability_hit: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ReactiveCompactor;

/// `prepare_turn` currently estimates prompt size with `prompt.len()`, i.e. chars not tokens.
/// Keep the auto-compact threshold materially above the provider prompt-cache floor so
/// full-context runs can still exercise prompt caching before local compaction intervenes.
pub const AUTO_COMPACT_INPUT_CHAR_LIMIT: usize = 16_384;

impl ReactiveCompactor {
    pub fn plan_auto_compact(
        &self,
        token_estimate: usize,
        limit: usize,
    ) -> Option<CompactServiceResult> {
        (token_estimate >= limit).then(|| CompactServiceResult {
            plan: CompactPlan {
                kind: CompactPlanKind::AutoCompact,
                notice_kind: "compaction",
                notice_message: "reactive compact requested before continuing the turn".into(),
                assistant_message: Some("compaction requested before continuing the turn".into()),
                retry_prompt: None,
            },
            next_step: CompactServiceNextStep::RetryReactiveCompact,
            tracking_key: "auto_compact",
            should_record_observability_hit: false,
        })
    }

    pub fn plan_stream_error_recovery(
        &self,
        has_attempted_reactive_compact: bool,
        has_attempted_collapse_drain: bool,
        error: Option<CompactRecoveryErrorContext<'_>>,
    ) -> CompactServiceResult {
        if !has_attempted_reactive_compact {
            let detail = error
                .map(|value| {
                    format!(
                        "reactive compact retry triggered after stream error [{}]: {}",
                        value.kind, value.message
                    )
                })
                .unwrap_or_else(|| "stream stop error triggered reactive compact retry".into());
            return CompactServiceResult {
                plan: CompactPlan {
                    kind: CompactPlanKind::ReactiveCompact,
                    notice_kind: "recovery",
                    notice_message: detail,
                    assistant_message: None,
                    retry_prompt: Some("Retry after reactive compact recovery.".into()),
                },
                next_step: CompactServiceNextStep::RetryReactiveCompact,
                tracking_key: "reactive_compact",
                should_record_observability_hit: true,
            };
        }

        if !has_attempted_collapse_drain {
            let detail = error
                .map(|value| {
                    format!(
                        "collapse drain retry triggered after repeated stream error [{}]: {}",
                        value.kind, value.message
                    )
                })
                .unwrap_or_else(|| "draining collapsed context before final model error".into());
            return CompactServiceResult {
                plan: CompactPlan {
                    kind: CompactPlanKind::CollapseDrain,
                    notice_kind: "recovery",
                    notice_message: detail,
                    assistant_message: None,
                    retry_prompt: Some("Retry after collapse drain recovery.".into()),
                },
                next_step: CompactServiceNextStep::RetryCollapseDrain,
                tracking_key: "collapse_drain",
                should_record_observability_hit: true,
            };
        }

        CompactServiceResult {
            plan: CompactPlan {
                kind: CompactPlanKind::Exhausted,
                notice_kind: "recovery",
                notice_message: error
                    .map(|value| {
                        format!(
                            "stream recovery exhausted after error [{}]: {}",
                            value.kind, value.message
                        )
                    })
                    .unwrap_or_else(|| "stream recovery exhausted after stop error".into()),
                assistant_message: None,
                retry_prompt: None,
            },
            next_step: CompactServiceNextStep::Exhausted,
            tracking_key: "exhausted",
            should_record_observability_hit: false,
        }
    }
}
