use anyhow::Result;
use clap::{Parser, Subcommand};
use esift_core::{
    checkpoint::CheckpointManager,
    config::{DestConfig, EsiftConfig},
    dest::{openobserve::OpenObserveDestination, stdout::StdoutDestination, Destination},
    source::{opensearch::OpenSearchSource, Source},
};
use esift_transform::mapping::Transformer;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "esift")]
#[command(about = "Extract and re-ingest data from Elasticsearch-compatible sources")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run from a TOML config file
    Run {
        #[arg(short, long, default_value = "esift.toml")]
        config: PathBuf,
    },
    /// Quick extraction with inline flags (no config file needed)
    Extract {
        #[arg(long, env = "ESIFT_SOURCE_URL")]
        source_url: String,

        #[arg(long)]
        source_index: String,

        #[arg(long, default_value = r#"{"match_all":{}}"#)]
        query: String,

        /// Destination: stdout or openobserve
        #[arg(long, default_value = "stdout")]
        dest: String,

        #[arg(long)]
        dest_url: Option<String>,

        #[arg(long, default_value = "default")]
        dest_org: String,

        #[arg(long)]
        dest_stream: Option<String>,

        #[arg(long, env = "ESIFT_DEST_USERNAME")]
        dest_username: Option<String>,

        #[arg(long, env = "ESIFT_DEST_PASSWORD")]
        dest_password: Option<String>,

        #[arg(long, default_value = "500")]
        batch_size: usize,

        #[arg(long, default_value = "./esift-checkpoint.json")]
        checkpoint: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("esift=info".parse()?),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            let content = std::fs::read_to_string(&config)?;
            let cfg: EsiftConfig = toml::from_str(&content)?;
            run_from_config(cfg).await?;
        }

        Commands::Extract {
            source_url,
            source_index,
            query,
            dest,
            dest_url,
            dest_org,
            dest_stream,
            dest_username,
            dest_password,
            batch_size,
            checkpoint,
        } => {
            let query_value: serde_json::Value = serde_json::from_str(&query)?;

            let mut source = OpenSearchSource::new(
                &source_url,
                &source_index,
                query_value,
                batch_size,
                None,
                None,
            )?;

            let mut destination: Box<dyn Destination> = match dest.as_str() {
                "stdout" => Box::new(StdoutDestination),
                "openobserve" | "oo" => {
                    let url = dest_url
                        .ok_or_else(|| anyhow::anyhow!("--dest-url required for openobserve"))?;
                    let stream = dest_stream
                        .ok_or_else(|| anyhow::anyhow!("--dest-stream required for openobserve"))?;
                    let username = dest_username
                        .ok_or_else(|| anyhow::anyhow!("--dest-username required for openobserve"))?;
                    let password = dest_password
                        .ok_or_else(|| anyhow::anyhow!("--dest-password required for openobserve"))?;
                    Box::new(OpenObserveDestination::new(
                        url, dest_org, stream, username, password,
                    )?)
                }
                other => anyhow::bail!(
                    "Unknown destination '{}'. Use 'stdout' or 'openobserve'",
                    other
                ),
            };

            let transformer = Transformer::identity();
            let mut checkpoint_mgr = CheckpointManager::new(checkpoint)?;

            run_extraction(&mut source, &mut *destination, &transformer, &mut checkpoint_mgr)
                .await?;
        }
    }

    Ok(())
}

async fn run_from_config(cfg: EsiftConfig) -> Result<()> {
    let query: serde_json::Value = serde_json::from_str(&cfg.source.query)?;

    let mut source = OpenSearchSource::new(
        &cfg.source.url,
        &cfg.source.index,
        query,
        cfg.source.batch_size,
        cfg.source.username,
        cfg.source.password,
    )?;

    let mut destination: Box<dyn Destination> = match cfg.destination {
        DestConfig::Stdout => Box::new(StdoutDestination),
        DestConfig::OpenObserve {
            url,
            org,
            stream,
            username,
            password,
        } => Box::new(OpenObserveDestination::new(
            url, org, stream, username, password,
        )?),
    };

    let transformer = Transformer::identity();
    let mut checkpoint_mgr = CheckpointManager::new(cfg.checkpoint_path)?;

    run_extraction(
        &mut source,
        &mut *destination,
        &transformer,
        &mut checkpoint_mgr,
    )
    .await
}

async fn run_extraction(
    source: &mut dyn Source,
    dest: &mut dyn Destination,
    transformer: &Transformer,
    checkpoint: &mut CheckpointManager,
) -> Result<()> {
    info!("Source:      {}", source.description());
    info!("Destination: {}", dest.description());

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .unwrap(),
    );

    source.open().await?;

    let mut total = checkpoint.state.docs_written;
    progress.set_message(format!("Starting ({} already written)", total));

    loop {
        match source.next_batch().await {
            Ok(Some(docs)) => {
                let transformed = transformer.apply_batch(docs);
                match dest.write_batch(transformed).await {
                    Ok(written) => {
                        checkpoint.state.record_batch(written, None);
                        checkpoint.save()?;
                        total += written as u64;
                        progress.set_message(format!("{} documents extracted", total));
                    }
                    Err(e) => {
                        error!("Write failed: {}", e);
                        source.close().await?;
                        return Err(e.into());
                    }
                }
            }
            Ok(None) => {
                progress.finish_with_message(format!("Done. {} documents extracted.", total));
                break;
            }
            Err(e) => {
                error!("Source error: {}", e);
                source.close().await?;
                return Err(e.into());
            }
        }
    }

    dest.flush().await?;
    source.close().await?;
    Ok(())
}
