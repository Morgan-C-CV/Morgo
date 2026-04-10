use std::sync::Arc;

use rust_agent::bootstrap::InteractionSurface;
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{CommandRouter, RouteDecision};

#[test]
fn router_executes_known_commands_before_query() {
    let registry = CommandRegistry::new().register(Arc::new(HelpCommand));
    let router = CommandRouter::new(registry);
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");

    assert_eq!(
        router.decide(&input),
        RouteDecision::ExecuteCommand("help".into())
    );
}

#[test]
fn router_falls_back_for_unknown_commands() {
    let router = CommandRouter::new(CommandRegistry::new());
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");

    assert_eq!(router.decide(&input), RouteDecision::ContinueToQuery);
}
