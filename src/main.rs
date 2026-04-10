use clap::Parser;
use rust_agent::bootstrap::{BootstrapCli, RuntimeBootstrap};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = BootstrapCli::parse();
    let runtime = RuntimeBootstrap::from_cli(cli);
    runtime.run().await
}
