use rust_agent::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};

#[tokio::test]
async fn startup_trace_contains_detect_surface_phase() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Print, false);
    state.enter_phase(BootstrapPhase::DetectSurface);
    state.enter_phase(BootstrapPhase::InjectSessionMetadata);

    assert!(state.startup_trace().contains("DetectSurface"));
}
