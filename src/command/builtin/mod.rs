pub mod clear;
pub mod compact;
pub mod config;
pub mod cost;
pub mod help;
pub mod plan;
pub mod resume;
pub mod session;
pub mod doctor;
pub mod mcp;
pub mod permissions;
pub mod skills;
pub mod status;
pub mod tasks;

use crate::command::registry::CommandRegistry;
use std::sync::Arc;

use clear::ClearCommand;
use compact::CompactCommand;
use config::ConfigCommand;
use cost::CostCommand;
use doctor::DoctorCommand;
use help::HelpCommand;
use mcp::McpCommand;
use permissions::PermissionsCommand;
use plan::PlanCommand;
use resume::ResumeCommand;
use session::SessionCommand;
use skills::SkillsCommand;
use status::StatusCommand;
use tasks::TasksCommand;

pub fn mount_core_commands(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(Arc::new(HelpCommand))
        .register(Arc::new(CostCommand))
        .register(Arc::new(CompactCommand))
        .register(Arc::new(ClearCommand))
        .register(Arc::new(ConfigCommand))
        .register(Arc::new(DoctorCommand))
        .register(Arc::new(McpCommand))
        .register(Arc::new(PermissionsCommand))
        .register(Arc::new(PlanCommand))
        .register(Arc::new(ResumeCommand))
        .register(Arc::new(SessionCommand))
        .register(Arc::new(SkillsCommand))
        .register(Arc::new(StatusCommand))
        .register(Arc::new(TasksCommand))
}

