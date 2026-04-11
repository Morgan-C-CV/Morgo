use std::sync::atomic::{AtomicBool, Ordering};

static COORDINATOR_MODE: AtomicBool = AtomicBool::new(false);

pub fn is_coordinator_mode() -> bool {
    COORDINATOR_MODE.load(Ordering::Relaxed)
}

pub fn set_coordinator_mode(enabled: bool) {
    COORDINATOR_MODE.store(enabled, Ordering::Relaxed);
}

pub fn match_session_mode(session_mode: Option<&str>) -> Option<String> {
    let Some(mode) = session_mode else {
        return None;
    };
    let target = mode == "coordinator";
    if is_coordinator_mode() == target {
        return None;
    }
    set_coordinator_mode(target);
    Some(if target {
        "Entered coordinator mode to match resumed session.".into()
    } else {
        "Exited coordinator mode to match resumed session.".into()
    })
}
