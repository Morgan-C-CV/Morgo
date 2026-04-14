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

#[derive(Debug, Clone, Default)]
pub struct ReactiveCompactor;

impl ReactiveCompactor {
    pub fn plan_auto_compact(&self, token_estimate: usize, limit: usize) -> Option<CompactPlan> {
        (token_estimate >= limit).then(|| CompactPlan {
            kind: CompactPlanKind::AutoCompact,
            notice_kind: "compaction",
            notice_message: "reactive compact requested before continuing the turn".into(),
            assistant_message: Some("compaction requested before continuing the turn".into()),
            retry_prompt: None,
        })
    }

    pub fn plan_stream_error_recovery(
        &self,
        has_attempted_reactive_compact: bool,
        has_attempted_collapse_drain: bool,
        error: Option<&str>,
    ) -> CompactPlan {
        if !has_attempted_reactive_compact {
            let detail = error
                .map(|value| {
                    format!("reactive compact retry triggered after stream error: {value}")
                })
                .unwrap_or_else(|| "stream stop error triggered reactive compact retry".into());
            return CompactPlan {
                kind: CompactPlanKind::ReactiveCompact,
                notice_kind: "recovery",
                notice_message: detail,
                assistant_message: None,
                retry_prompt: Some("Retry after reactive compact recovery.".into()),
            };
        }

        if !has_attempted_collapse_drain {
            let detail = error
                .map(|value| {
                    format!("collapse drain retry triggered after repeated stream error: {value}")
                })
                .unwrap_or_else(|| "draining collapsed context before final model error".into());
            return CompactPlan {
                kind: CompactPlanKind::CollapseDrain,
                notice_kind: "recovery",
                notice_message: detail,
                assistant_message: None,
                retry_prompt: Some("Retry after collapse drain recovery.".into()),
            };
        }

        CompactPlan {
            kind: CompactPlanKind::Exhausted,
            notice_kind: "recovery",
            notice_message: error
                .map(|value| format!("stream recovery exhausted after error: {value}"))
                .unwrap_or_else(|| "stream recovery exhausted after stop error".into()),
            assistant_message: None,
            retry_prompt: None,
        }
    }
}
