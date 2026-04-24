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
            description: "macOS computer-use actions (screenshot only in v1)".into(),
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
        let subcommand = input.command_args.split_whitespace().next().unwrap_or("");

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
            "stop" => Ok(CommandResult::Message(
                "no active computer-use session".into(),
            )),
            _ => Ok(CommandResult::Message(usage())),
        }
    }
}

fn usage() -> String {
    "usage: /computer <subcommand>\n  screenshot  capture the screen to a temp file\n  stop        stop an active computer-use session".into()
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

fn image_dimensions(path: &str) -> anyhow::Result<(u32, u32)> {
    let reader = image::ImageReader::open(path)?.with_guessed_format()?;
    let (w, h) = reader.into_dimensions()?;
    Ok((w, h))
}
