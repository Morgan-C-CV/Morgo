use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ComputerCommand;

#[async_trait]
impl Command for ComputerCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "computer".into(),
            description: "CLI-only computer control with observation-first, explicitly commanded actions".into(),
            source: CommandSource::Builtin,
            category: "system".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::CliOnly,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: true,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let mut args = input.command_args.split_whitespace();
        let subcommand = args.next().unwrap_or("");

        match subcommand {
            "screenshot" => {
                let path = screenshot_path();
                match take_screenshot(&path) {
                    Ok(result) => Ok(CommandResult::Message(format!(
                        "screenshot saved: {}\n  size: {}x{} px, {} bytes",
                        result.path, result.width, result.height, result.bytes
                    ))),
                    Err(e) => Ok(CommandResult::Message(format!("screenshot failed: {e}"))),
                }
            }
            "click" => {
                let point = parse_point(args.next(), args.next());
                match point {
                    Some(p) => match pointer_click(p) {
                        Ok(()) => Ok(CommandResult::Message(format!(
                            "clicked at ({}, {})",
                            p.x, p.y
                        ))),
                        Err(e) => Ok(CommandResult::Message(format!("click failed: {e}"))),
                    },
                    None => Ok(CommandResult::Message(usage())),
                }
            }
            "move" => {
                let point = parse_point(args.next(), args.next());
                match point {
                    Some(p) => match pointer_move(p) {
                        Ok(()) => Ok(CommandResult::Message(format!(
                            "moved to ({}, {})",
                            p.x, p.y
                        ))),
                        Err(e) => Ok(CommandResult::Message(format!("move failed: {e}"))),
                    },
                    None => Ok(CommandResult::Message(usage())),
                }
            }
            "stop" => Ok(CommandResult::Message(
                "no active computer-use session; stop is currently a no-op control command"
                    .into(),
            )),
            _ => Ok(CommandResult::Message(usage())),
        }
    }
}

fn usage() -> String {
    "usage: /computer <subcommand>\n  screenshot       capture the screen to a temp file (primary observation entry point)\n  click <x> <y>   click at explicit absolute screen coordinates\n  move <x> <y>    move the pointer to explicit absolute screen coordinates (no click)\n  stop             no active computer-use session; no-op control command\n\nconstraints:\n  - CLI-only, sensitive, explicitly commanded actions only\n  - screenshot is the primary observation entry point\n  - no typing, no hotkeys, no autonomous multi-step execution\n  - no generic agent tool exposure and no remote surface access"
        .into()
}

#[derive(Clone, Copy)]
struct Point {
    x: i32,
    y: i32,
}

fn parse_point(x: Option<&str>, y: Option<&str>) -> Option<Point> {
    let x = x?.parse::<i32>().ok()?;
    let y = y?.parse::<i32>().ok()?;
    Some(Point { x, y })
}

fn screenshot_path() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir()
        .join(format!("rust_agent_screenshot_{ts}.png"))
        .to_string_lossy()
        .into_owned()
}

struct ScreenshotResult {
    path: String,
    width: u32,
    height: u32,
    bytes: u64,
}

#[cfg(target_os = "macos")]
fn take_screenshot(output_path: &str) -> anyhow::Result<ScreenshotResult> {
    let status = std::process::Command::new("screencapture")
        .args(["-x", output_path])
        .status()?;
    if !status.success() {
        anyhow::bail!("screencapture exited with status {status}");
    }
    let meta = std::fs::metadata(output_path)?;
    let bytes = meta.len();
    let (width, height) = image_dimensions(output_path)?;
    Ok(ScreenshotResult {
        path: output_path.to_string(),
        width,
        height,
        bytes,
    })
}

#[cfg(not(target_os = "macos"))]
fn take_screenshot(_output_path: &str) -> anyhow::Result<ScreenshotResult> {
    anyhow::bail!("computer-use is only supported on macOS")
}

#[cfg(target_os = "macos")]
fn pointer_click(p: Point) -> anyhow::Result<()> {
    let script = format!(
        "tell application \"System Events\" to click at {{{}, {}}}",
        p.x, p.y
    );
    run_osascript(&script)
}

#[cfg(not(target_os = "macos"))]
fn pointer_click(_p: Point) -> anyhow::Result<()> {
    anyhow::bail!("computer-use is only supported on macOS")
}

#[cfg(target_os = "macos")]
fn pointer_move(p: Point) -> anyhow::Result<()> {
    let script = format!(
        "tell application \"System Events\" to set the position of the mouse to {{{}, {}}}",
        p.x, p.y
    );
    run_osascript(&script)
}

#[cfg(not(target_os = "macos"))]
fn pointer_move(_p: Point) -> anyhow::Result<()> {
    anyhow::bail!("computer-use is only supported on macOS")
}

#[cfg(target_os = "macos")]
fn run_osascript(script: &str) -> anyhow::Result<()> {
    let output = std::process::Command::new("osascript")
        .args(["-e", script])
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let detail = if !stderr.trim().is_empty() {
        stderr.trim().to_string()
    } else if !stdout.trim().is_empty() {
        stdout.trim().to_string()
    } else {
        format!("osascript exited with status {}", output.status)
    };
    anyhow::bail!("{detail}")
}

fn image_dimensions(path: &str) -> anyhow::Result<(u32, u32)> {
    let reader = image::ImageReader::open(path)?.with_guessed_format()?;
    let (w, h) = reader.into_dimensions()?;
    Ok((w, h))
}
