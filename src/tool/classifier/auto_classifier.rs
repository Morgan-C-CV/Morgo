#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifierDecision {
    Allow,
    Ask { code: &'static str, warning: String },
    Deny { code: &'static str, warning: String },
}

pub fn classify_bash_command(command: &str) -> ClassifierDecision {
    let lowered = command.to_ascii_lowercase();

    if lowered.contains("curl ") && (lowered.contains("| sh") || lowered.contains("| bash")) {
        return ClassifierDecision::Deny {
            code: "download_and_exec",
            warning: "download-and-exec pattern detected".into(),
        };
    }
    if lowered.contains("token") || lowered.contains("credential") || lowered.contains("id_rsa") {
        return ClassifierDecision::Ask {
            code: "secret_access",
            warning: "command may access credentials or secrets".into(),
        };
    }
    if lowered.contains("sudo") || lowered.contains("launchctl") {
        return ClassifierDecision::Ask {
            code: "privileged_system",
            warning: "command touches privileged system state".into(),
        };
    }

    ClassifierDecision::Allow
}
