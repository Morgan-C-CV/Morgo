use std::sync::Arc;

use rust_agent::bootstrap::InteractionSurface;
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{CommandRouter, RouteDecision};
use rust_agent::security::authorizer::DefaultSurfaceAuthorizer;

#[test]
fn router_executes_known_commands_before_query() {
    let registry = CommandRegistry::new().register(Arc::new(HelpCommand));
    let router = CommandRouter::new(registry, Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");

    assert_eq!(
        router.decide(&input),
        RouteDecision::ExecuteCommand("help".into())
    );
}

#[test]
fn router_falls_back_for_unknown_commands() {
    let router = CommandRouter::new(CommandRegistry::new(), Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");

    assert_eq!(router.decide(&input), RouteDecision::ContinueToQuery);
}

#[test]
fn router_denies_unauthenticated_remote_actor() {
    let router = CommandRouter::new(CommandRegistry::new(), Box::new(DefaultSurfaceAuthorizer));
    let mut input = NormalizedInput::from_raw(InteractionSurface::Remote, "/help");
    input.actor.is_authenticated = false;

    assert_eq!(
        router.decide(&input),
        RouteDecision::Deny("unauthenticated actor for remote surface".into())
    );
}
