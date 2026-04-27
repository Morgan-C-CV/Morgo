pub mod clear;
pub mod compact;
pub mod computer;
pub mod config;
pub mod cost;
pub mod doctor;
pub mod help;
pub mod lism;
pub mod mcp;
pub mod model;
pub mod permissions;
pub mod plan;
pub mod plugins;
pub mod resume;
pub mod session;
pub mod skills;
pub mod status;
pub mod swarm;
pub mod tasks;

use crate::command::registry::CommandRegistry;
use std::sync::Arc;

use clear::ClearCommand;
use compact::CompactCommand;
use computer::ComputerCommand;
use config::ConfigCommand;
use cost::CostCommand;
use doctor::DoctorCommand;
use help::HelpCommand;
use lism::LisMCommand;
use mcp::McpCommand;
use model::ModelCommand;
use permissions::PermissionsCommand;
use plan::PlanCommand;
use plugins::PluginsCommand;
use resume::ResumeCommand;
use session::SessionCommand;
use skills::SkillsCommand;
use status::StatusCommand;
use swarm::SwarmCommand;
use tasks::TasksCommand;

pub fn register_builtin_commands(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(Arc::new(HelpCommand))
        .register(Arc::new(LisMCommand))
        .register(Arc::new(CostCommand))
        .register(Arc::new(CompactCommand))
        .register(Arc::new(ClearCommand))
        .register(Arc::new(ComputerCommand))
        .register(Arc::new(ConfigCommand))
        .register(Arc::new(DoctorCommand))
        .register(Arc::new(ModelCommand))
        .register(Arc::new(PermissionsCommand))
        .register(Arc::new(PlanCommand))
        .register(Arc::new(PluginsCommand))
        .register(Arc::new(ResumeCommand))
        .register(Arc::new(SessionCommand))
        .register(Arc::new(SkillsCommand))
        .register(Arc::new(StatusCommand))
        .register(Arc::new(SwarmCommand))
        .register(Arc::new(TasksCommand))
}

pub fn register_mcp_commands(registry: CommandRegistry) -> CommandRegistry {
    registry.register(Arc::new(McpCommand))
}
