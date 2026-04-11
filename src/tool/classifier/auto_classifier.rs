#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifierDecision {
    Allow,
    Ask(String),
    Deny(String),
}

pub fn classify_bash_command(command: &str) -> ClassifierDecision {
    let lowered = command.to_ascii_lowercase();

    if lowered.contains("curl ") && (lowered.contains("| sh") || lowered.contains("| bash")) {
        return ClassifierDecision::Deny("download-and-exec pattern detected".into());
    }
    if lowered.contains("token") || lowered.contains("credential") || lowered.contains("id_rsa") {
        return ClassifierDecision::Ask("command may access credentials or secrets".into());
    }
    if lowered.contains("sudo") || lowered.contains("launchctl") {
        return ClassifierDecision::Ask("command touches privileged system state".into());
    }

    ClassifierDecision::Allow
}
