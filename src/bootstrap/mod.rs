pub mod config_root;
pub mod model_profiles;
mod runtime;
mod setup;
mod state;
pub mod teammate_registry;
pub mod warnings;

pub use runtime::{
    BootstrapCli, FinalizedRuntime, PromptAugmentation, PromptAugmentationMetadata,
    RuntimeBootstrap, RuntimeInitializeBundle, ShutdownFailure, ShutdownOutcome,
    UserAccessDecision, execute_runtime_shutdown, execute_runtime_shutdown_with_deadline,
    has_explicit_provider_env_override, is_tui_exit_input, runtime_shutdown_timeout,
    summarize_active_model_provider, tui_clear_screen_prefix,
};
pub use setup::SetupContext;
pub use state::{
    BootstrapPhase, BootstrapState, ClientType, InteractionSurface, SessionMode, SessionSource,
};
pub use warnings::{StartupWarning, StartupWarnings};
