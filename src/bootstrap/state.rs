use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InteractionSurface {
    Cli,
    Telegram,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionMode {
    Interactive,
    Headless,
    Print,
    InitOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    Cli,
    Bot,
    RemoteControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSource {
    LocalCli,
    Telegram,
    RemoteControl,
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
    pub client_type: ClientType,
    pub session_source: SessionSource,
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
        let (client_type, session_source) = match surface {
            InteractionSurface::Cli => (ClientType::Cli, SessionSource::LocalCli),
            InteractionSurface::Telegram => (ClientType::Bot, SessionSource::Telegram),
            InteractionSurface::Remote => (ClientType::RemoteControl, SessionSource::RemoteControl),
        };
        Self {
            surface,
            session_mode,
            client_type,
            session_source,
            original_cwd: cwd.clone(),
            current_cwd: cwd,
            phases: Vec::new(),
            trace_startup,
        }
    }

    pub fn enter_phase(&mut self, phase: BootstrapPhase) {
        self.record_phase(phase);
    }

    pub fn record_phase(&mut self, phase: BootstrapPhase) {
        self.phases.push(phase);
    }

    pub fn finalize(mut self) -> Self {
        self.record_phase(BootstrapPhase::FinalizeState);
        self
    }

    pub fn current_phase(&self) -> Option<BootstrapPhase> {
        self.phases.last().copied()
    }

    pub fn startup_trace(&self) -> String {
        self.phases
            .iter()
            .map(|phase| format!("{phase:?}"))
            .collect::<Vec<_>>()
            .join(" -> ")
    }
}
