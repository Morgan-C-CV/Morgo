use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionSurface {
    Cli,
    Telegram,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMode {
    Interactive,
    Headless,
    Print,
    InitOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BootstrapPhase {
    DetectSurface,
    InjectSessionMetadata,
    ResolvePermissions,
    BuildToolContext,
    AssembleTools,
    Setup,
    InitializeRuntime,
    AugmentPrompt,
    GateUserAccess,
    FinalizeState,
}

#[derive(Debug, Clone)]
pub struct BootstrapState {
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub original_cwd: PathBuf,
    pub current_cwd: PathBuf,
    pub phases: Vec<BootstrapPhase>,
    pub trace_startup: bool,
}

impl BootstrapState {
    pub fn new(
        surface: InteractionSurface,
        session_mode: SessionMode,
        trace_startup: bool,
    ) -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            surface,
            session_mode,
            original_cwd: cwd.clone(),
            current_cwd: cwd,
            phases: Vec::new(),
            trace_startup,
        }
    }

    pub fn enter_phase(&mut self, phase: BootstrapPhase) {
        self.phases.push(phase);
    }

    pub fn startup_trace(&self) -> String {
        self.phases
            .iter()
            .map(|phase| format!("{phase:?}"))
            .collect::<Vec<_>>()
            .join(" -> ")
    }
}
