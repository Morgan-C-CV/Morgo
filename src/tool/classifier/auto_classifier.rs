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
    if contains_secret_access_pattern(&lowered) {
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

fn contains_secret_access_pattern(lowered: &str) -> bool {
    [
        "id_rsa",
        "id_ed25519",
        ".env",
        "api_key",
        "apikey",
        "access_key",
        "secret_key",
        "credentials",
        "credential",
        "bearer ",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
}
