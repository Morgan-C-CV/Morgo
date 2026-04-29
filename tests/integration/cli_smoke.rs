use rust_agent::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};

#[path = "plan_resume_flow.rs"]
mod plan_resume_flow;
#[path = "plugin_flow.rs"]
mod plugin_flow;
#[path = "remote_flow.rs"]
mod remote_flow;
#[path = "skills_visibility.rs"]
mod skills_visibility;
#[path = "telegram_transport_flow.rs"]
mod telegram_transport_flow;
#[path = "web_flow.rs"]
mod web_flow;

#[tokio::test]
async fn startup_trace_contains_detect_surface_phase() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Print, false);
    state.enter_phase(BootstrapPhase::DetectSurface);
    state.enter_phase(BootstrapPhase::InjectSessionMetadata);

    assert!(state.startup_trace().contains("DetectSurface"));
}
