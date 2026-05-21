use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::process::Command;

use crate::security::sandbox_config::SandboxConfig;
use crate::tool::builtin::bash::clamped_reader::{ClampedOutput, read_clamped};
use crate::tool::builtin::bash::readonly_validation::is_read_only_command;
use crate::tool::builtin::bash::security::{contains_destructive_pattern, contains_shell_operator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPolicy {
    Disabled,
    WorkspaceWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxRunner {
    DirectShell,
    Bubblewrap,
    MacSeatbelt,
}

impl SandboxRunner {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectShell => "direct_shell",
            Self::Bubblewrap => "bubblewrap",
            Self::MacSeatbelt => "mac_seatbelt",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SandboxExecutionPlan {
    runner: SandboxRunner,
    policy: SandboxPolicy,
    config: Arc<SandboxConfig>,
}

pub struct ClampedProcessOutput {
    pub status: std::process::ExitStatus,
    pub stdout: ClampedOutput,
    pub stderr: ClampedOutput,
    pub runner: SandboxRunner,
    pub sandbox_enabled: bool,
    pub sandbox_policy: SandboxPolicy,
}

pub fn select_sandbox_policy(command: &str) -> SandboxPolicy {
    if contains_destructive_pattern(command) || contains_shell_operator(command) {
        SandboxPolicy::WorkspaceWrite
    } else if is_read_only_command(command) {
        SandboxPolicy::ReadOnly
    } else {
        SandboxPolicy::WorkspaceWrite
    }
}

pub async fn execute_with_sandbox(
    command: &str,
    cwd: &Path,
    policy: SandboxPolicy,
) -> anyhow::Result<ClampedProcessOutput> {
    execute_with_sandbox_config(command, cwd, policy, Arc::new(SandboxConfig::default())).await
}

pub async fn execute_with_sandbox_config(
    command: &str,
    cwd: &Path,
    policy: SandboxPolicy,
    config: Arc<SandboxConfig>,
) -> anyhow::Result<ClampedProcessOutput> {
    let plan = build_execution_plan(policy, config)?;
    let (mut process, profile_path) = build_command(&plan, command, cwd);
    let mut child = process
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to execute bash command: {e}"))?;

    let stdout_task = child.stdout.take().map(|s| tokio::spawn(read_clamped(s)));
    let stderr_task = child.stderr.take().map(|s| tokio::spawn(read_clamped(s)));

    let status = child
        .wait()
        .await
        .map_err(|e| anyhow::anyhow!("failed to wait for bash command: {e}"))?;

    let empty = || ClampedOutput {
        head: vec![],
        tail: vec![],
        truncated: false,
        total_bytes_read: 0,
    };
    let stdout = match stdout_task {
        Some(t) => t.await.map_err(|e| anyhow::anyhow!("stdout join: {e}"))?,
        None => empty(),
    };
    let stderr = match stderr_task {
        Some(t) => t.await.map_err(|e| anyhow::anyhow!("stderr join: {e}"))?,
        None => empty(),
    };

    if let Some(profile_path) = &profile_path {
        let _ = std::fs::remove_file(profile_path);
    }

    Ok(ClampedProcessOutput {
        status,
        stdout,
        stderr,
        runner: plan.runner,
        sandbox_enabled: plan.config.enabled && !matches!(plan.runner, SandboxRunner::DirectShell),
        sandbox_policy: policy,
    })
}

fn build_execution_plan(
    policy: SandboxPolicy,
    config: Arc<SandboxConfig>,
) -> anyhow::Result<SandboxExecutionPlan> {
    if matches!(policy, SandboxPolicy::Disabled) {
        if config.enabled && !config.allow_unsandboxed_commands {
            anyhow::bail!("sandbox disabled by command but unsandboxed commands are not allowed");
        }
        return Ok(SandboxExecutionPlan {
            runner: SandboxRunner::DirectShell,
            policy,
            config,
        });
    }

    if !config.enabled {
        return Ok(SandboxExecutionPlan {
            runner: SandboxRunner::DirectShell,
            policy,
            config,
        });
    }

    let runner = match platform_runner() {
        Ok(runner) => runner,
        Err(error) if !config.fail_if_unavailable && config.allow_unsandboxed_commands => {
            tracing::warn!(
                "{error}; falling back to direct shell because fail_if_unavailable=false"
            );
            SandboxRunner::DirectShell
        }
        Err(error) => return Err(error),
    };
    Ok(SandboxExecutionPlan {
        runner,
        policy,
        config,
    })
}

fn platform_runner() -> anyhow::Result<SandboxRunner> {
    if cfg!(target_os = "linux") {
        if find_in_path("bwrap").is_none() {
            anyhow::bail!("sandbox unavailable: bwrap not found");
        }
        return Ok(SandboxRunner::Bubblewrap);
    }
    if cfg!(target_os = "macos") {
        if find_in_path("sandbox-exec").is_none() {
            anyhow::bail!("sandbox unavailable: sandbox-exec not found");
        }
        return Ok(SandboxRunner::MacSeatbelt);
    }
    anyhow::bail!("sandbox unavailable: unsupported platform")
}

fn build_command(
    plan: &SandboxExecutionPlan,
    command: &str,
    cwd: &Path,
) -> (Command, Option<PathBuf>) {
    let mut profile_path = None;
    let mut process = match plan.runner {
        SandboxRunner::DirectShell => direct_shell_command(command),
        SandboxRunner::Bubblewrap => bubblewrap_command(plan, command, cwd),
        SandboxRunner::MacSeatbelt => mac_seatbelt_command(plan, command, cwd, &mut profile_path),
    };

    process
        .current_dir(cwd)
        .stdin(Stdio::null())
        .env("RUST_AGENT_SANDBOX_POLICY", format!("{:?}", plan.policy))
        .env("RUST_AGENT_SANDBOX_RUNNER", plan.runner.as_str())
        .env(
            "RUST_AGENT_SANDBOX_ENABLED",
            if plan.config.enabled && !matches!(plan.runner, SandboxRunner::DirectShell) {
                "1"
            } else {
                "0"
            },
        );
    (process, profile_path)
}

fn direct_shell_command(command: &str) -> Command {
    let mut process = Command::new("/bin/sh");
    process.kill_on_drop(true);
    process.arg("-lc").arg(command);
    process
}

fn bubblewrap_command(plan: &SandboxExecutionPlan, command: &str, cwd: &Path) -> Command {
    let mut process = Command::new("bwrap");
    process.kill_on_drop(true);
    process.args(["--die-with-parent", "--unshare-pid"]);
    bind_read_only_if_exists(&mut process, "/usr");
    bind_read_only_if_exists(&mut process, "/bin");
    bind_read_only_if_exists(&mut process, "/sbin");
    bind_read_only_if_exists(&mut process, "/lib");
    bind_read_only_if_exists(&mut process, "/lib64");
    bind_read_only_if_exists(&mut process, "/etc");
    bind_read_only_if_exists(&mut process, "/opt");
    bind_read_only_if_exists(&mut process, "/nix");
    if Path::new("/dev").exists() {
        process.args(["--dev-bind", "/dev", "/dev"]);
    }
    process.args(["--proc", "/proc", "--tmpfs", "/tmp"]);

    let temp = std::env::temp_dir();
    bind_path(
        &mut process,
        &temp,
        !matches!(plan.policy, SandboxPolicy::ReadOnly),
    );
    bind_path(
        &mut process,
        cwd,
        matches!(plan.policy, SandboxPolicy::WorkspaceWrite),
    );
    for path in &plan.config.filesystem.allow_read {
        bind_path(&mut process, path, false);
    }
    if matches!(plan.policy, SandboxPolicy::WorkspaceWrite) {
        for path in &plan.config.filesystem.allow_write {
            bind_path(&mut process, path, true);
        }
    }
    for path in &plan.config.filesystem.deny_read {
        if path.exists() {
            process.arg("--tmpfs").arg(path);
        }
    }
    for path in &plan.config.filesystem.deny_write {
        if path.exists() {
            bind_path(&mut process, path, false);
        }
    }

    process.args(["/bin/sh", "-lc", command]);
    process
}

fn mac_seatbelt_command(
    plan: &SandboxExecutionPlan,
    command: &str,
    cwd: &Path,
    profile_path_slot: &mut Option<PathBuf>,
) -> Command {
    let profile = build_seatbelt_profile(plan, cwd);
    let profile_path = write_seatbelt_profile(&profile).ok();
    let mut process = Command::new("sandbox-exec");
    process.kill_on_drop(true);
    if let Some(path) = &profile_path {
        process.arg("-f").arg(path);
        *profile_path_slot = profile_path;
    } else {
        process.arg("-p").arg("(version 1)(deny default)");
    }
    process.args(["/bin/sh", "-lc", command]);
    process
}

fn bind_read_only_if_exists(process: &mut Command, path: &str) {
    let path = Path::new(path);
    if path.exists() {
        process.arg("--ro-bind").arg(path).arg(path);
    }
}

fn bind_path(process: &mut Command, path: &Path, writable: bool) {
    if !path.exists() {
        return;
    }
    add_bwrap_parent_dirs(process, path);
    if writable {
        process.arg("--bind").arg(path).arg(path);
    } else {
        process.arg("--ro-bind").arg(path).arg(path);
    }
}

fn add_bwrap_parent_dirs(process: &mut Command, path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    let mut current = PathBuf::new();
    for component in parent.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() || current == Path::new("/") {
            continue;
        }
        process.arg("--dir").arg(&current);
    }
}

fn build_seatbelt_profile(plan: &SandboxExecutionPlan, cwd: &Path) -> String {
    let mut profile = String::from(
        "(version 1)\n\
         (deny default)\n\
         (allow process*)\n\
         (allow sysctl*)\n\
         (allow file-read-metadata)\n\
         (allow file-read* (subpath \"/bin\") (subpath \"/sbin\") (subpath \"/usr\") (subpath \"/System\") (subpath \"/Library\") (subpath \"/private/etc\") (subpath \"/dev\"))\n",
    );
    profile.push_str(&format!(
        "(allow file-read* (subpath \"{}\"))\n",
        escape_seatbelt_path(cwd)
    ));
    for path in &plan.config.filesystem.allow_read {
        profile.push_str(&format!(
            "(allow file-read* (subpath \"{}\"))\n",
            escape_seatbelt_path(path)
        ));
    }

    if matches!(plan.policy, SandboxPolicy::WorkspaceWrite) {
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape_seatbelt_path(cwd)
        ));
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape_seatbelt_path(&std::env::temp_dir())
        ));
        for path in &plan.config.filesystem.allow_write {
            profile.push_str(&format!(
                "(allow file-write* (subpath \"{}\"))\n",
                escape_seatbelt_path(path)
            ));
        }
    } else {
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape_seatbelt_path(&std::env::temp_dir())
        ));
    }

    for path in &plan.config.filesystem.deny_read {
        profile.push_str(&format!(
            "(deny file-read* (subpath \"{}\"))\n",
            escape_seatbelt_path(path)
        ));
    }
    for path in &plan.config.filesystem.deny_write {
        profile.push_str(&format!(
            "(deny file-write* (subpath \"{}\"))\n",
            escape_seatbelt_path(path)
        ));
    }
    profile
}

fn write_seatbelt_profile(profile: &str) -> std::io::Result<PathBuf> {
    let mut path = std::env::temp_dir();
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!("morgo-seatbelt-{id}.sb"));
    std::fs::write(&path, profile)?;
    Ok(path)
}

fn escape_seatbelt_path(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn find_in_path(binary: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths).find_map(|dir| {
        let candidate = dir.join(binary);
        if is_executable_file(&candidate) {
            Some(candidate)
        } else {
            None
        }
    })
}

fn is_executable_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[allow(dead_code)]
fn _args_debug(args: &[OsString]) -> Vec<String> {
    args.iter()
        .map(|arg| arg.to_string_lossy().into())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_missing_platform_runner_fails_closed() {
        if cfg!(target_os = "linux") && find_in_path("bwrap").is_some() {
            return;
        }
        if cfg!(target_os = "macos") && find_in_path("sandbox-exec").is_some() {
            return;
        }
        let config = Arc::new(SandboxConfig {
            enabled: true,
            fail_if_unavailable: true,
            allow_unsandboxed_commands: false,
            filesystem: Default::default(),
        });

        let err = build_execution_plan(SandboxPolicy::WorkspaceWrite, config)
            .expect_err("sandbox should fail closed without a runner");
        assert!(err.to_string().contains("sandbox unavailable"));
    }

    #[test]
    fn unsandboxed_command_can_be_blocked_by_config() {
        let config = Arc::new(SandboxConfig {
            enabled: true,
            fail_if_unavailable: true,
            allow_unsandboxed_commands: false,
            filesystem: Default::default(),
        });

        let err = build_execution_plan(SandboxPolicy::Disabled, config)
            .expect_err("disabled policy should be rejected");
        assert!(
            err.to_string()
                .contains("unsandboxed commands are not allowed")
        );
    }

    #[test]
    fn runner_unavailable_can_fallback_only_when_config_allows_it() {
        if cfg!(target_os = "linux") && find_in_path("bwrap").is_some() {
            return;
        }
        if cfg!(target_os = "macos") && find_in_path("sandbox-exec").is_some() {
            return;
        }
        let config = Arc::new(SandboxConfig {
            enabled: true,
            fail_if_unavailable: false,
            allow_unsandboxed_commands: true,
            filesystem: Default::default(),
        });

        let plan = build_execution_plan(SandboxPolicy::WorkspaceWrite, config)
            .expect("fallback should be allowed");
        assert_eq!(plan.runner, SandboxRunner::DirectShell);
    }
}
