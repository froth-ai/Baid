//! The `baid` binary.

use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use server::cli::{Cli, Command, ServeArgs};
use server::config::{load, resolve_secrets, Overrides};
use server::run::{serve, shutdown_on_signal};
use server::state::{credentials, State};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("BAID_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    server::failopen::install_panic_hook();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let args = match cli.command {
        None => ServeArgs::default(),
        Some(Command::Serve(a)) => a,
        Some(Command::CheckConfig) => {
            let config = load(&cli.config, &Overrides::default())?;
            let secrets = resolve_secrets(&config, |k| std::env::var(k).ok())?;
            adapters::Registry::from_config(&config, |t| credentials(&secrets, t)).await?;
            println!(
                "{}: ok ({} tenant(s))",
                cli.config.display(),
                config.tenants.len()
            );
            return Ok(());
        }
    };
    let overrides = Overrides {
        listen: args.listen,
        admin_listen: args.admin_listen,
    };
    let config = load(&cli.config, &overrides)?;
    let secrets = resolve_secrets(&config, |k| std::env::var(k).ok())?;

    let public = TcpListener::bind(config.server.listen)
        .await
        .with_context(|| format!("binding {}", config.server.listen))?;
    let admin = TcpListener::bind(config.server.admin_listen)
        .await
        .with_context(|| format!("binding {}", config.server.admin_listen))?;
    info!(
        listen = %config.server.listen,
        admin_listen = %config.server.admin_listen,
        tenants = config.tenants.len(),
        "baid started"
    );

    let state = State::build(config, secrets).await?;
    let shutdown = CancellationToken::new();
    let server = serve(state.clone(), public, admin, shutdown.clone());
    tokio::pin!(server);
    tokio::select! {
        r = &mut server => r?,
        () = shutdown_on_signal(state, shutdown) => server.await?,
    }
    info!("stopped");
    Ok(())
}
