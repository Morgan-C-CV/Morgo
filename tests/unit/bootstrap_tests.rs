use rust_agent::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};

#[test]
fn bootstrap_state_records_phase_order() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, true);
    state.enter_phase(BootstrapPhase::DetectSurface);
    state.enter_phase(BootstrapPhase::ResolvePermissions);
    state.enter_phase(BootstrapPhase::FinalizeState);

    assert_eq!(
        state.startup_trace(),
        "DetectSurface -> ResolvePermissions -> FinalizeState"
    );
}
