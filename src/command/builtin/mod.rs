pub mod clear;
pub mod compact;
pub mod config;
pub mod cost;
pub mod help;
pub mod plan;
pub mod resume;
pub mod session;
pub mod status;
pub mod tasks;

use crate::command::registry::CommandRegistry;
use std::sync::Arc;

use clear::ClearCommand;
use compact::CompactCommand;
use config::ConfigCommand;
use cost::CostCommand;
use help::HelpCommand;
use plan::PlanCommand;
use resume::ResumeCommand;
use session::SessionCommand;
use status::StatusCommand;
use tasks::TasksCommand;

pub fn mount_core_commands(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(Arc::new(HelpCommand))
        .register(Arc::new(CostCommand))
        .register(Arc::new(CompactCommand))
        .register(Arc::new(ClearCommand))
        .register(Arc::new(ConfigCommand))
        .register(Arc::new(PlanCommand))
        .register(Arc::new(ResumeCommand))
        .register(Arc::new(SessionCommand))
        .register(Arc::new(StatusCommand))
        .register(Arc::new(TasksCommand))
}

