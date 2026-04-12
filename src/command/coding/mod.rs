pub mod commit;
pub mod context;
pub mod diff;
pub mod review;

use crate::command::registry::CommandRegistry;
use std::sync::Arc;

use commit::CommitCommand;
use context::ContextCommand;
use diff::DiffCommand;
use review::ReviewCommand;

pub fn register_coding_commands(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(Arc::new(DiffCommand))
        .register(Arc::new(CommitCommand))
        .register(Arc::new(ReviewCommand))
        .register(Arc::new(ContextCommand))
}
