pub mod config_root;
mod runtime;
mod setup;
mod state;

pub use runtime::{
    BootstrapCli, FinalizedRuntime, PromptAugmentation, PromptAugmentationMetadata,
    RuntimeBootstrap, RuntimeInitializeBundle, UserAccessDecision, is_tui_exit_input,
    tui_clear_screen_prefix,
};
pub use setup::SetupContext;
pub use state::{
    BootstrapPhase, BootstrapState, ClientType, InteractionSurface, SessionMode, SessionSource,
};
