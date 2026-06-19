use anyhow::Result;
use clap::{Parser, Subcommand};
use esift_core::{
    checkpoint::CheckpointManager,
    config::{DestConfig, EsiftConfig},
    dest::{
        openobserve::{OpenObserveDestination, OpenObserveOptions},
        stdout::StdoutDestination,
        Destination,
    },
    source::{
        opensearch::{Auth, OpenSearchSource},
        Source,
    },
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

#[allow(clippy::large_enum_variant)]
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

        /// Source authentication type: basic, aws-sigv4, none
        #[arg(long, env = "ESIFT_SOURCE_AUTH_TYPE")]
        source_auth_type: Option<String>,

        /// AWS region for Signature Version 4 signing
        #[arg(long, env = "ESIFT_SOURCE_AWS_REGION")]
        source_aws_region: Option<String>,

        /// Source username (for basic auth)
        #[arg(long, env = "ESIFT_SOURCE_USERNAME")]
        source_username: Option<String>,

        /// Source password (for basic auth)
        #[arg(long, env = "ESIFT_SOURCE_PASSWORD")]
        source_password: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("esift=info".parse()?),
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
            source_auth_type,
            source_aws_region,
            source_username,
            source_password,
        } => {
            let query_value: serde_json::Value = serde_json::from_str(&query)?;

            let auth = resolve_auth(
                source_auth_type.as_deref(),
                source_username,
                source_password,
                source_aws_region,
            )
            .await?;

            let mut checkpoint_mgr = CheckpointManager::new(checkpoint)?;

            let mut source = OpenSearchSource::new(
                &source_url,
                &source_index,
                query_value,
                batch_size,
                auth,
                checkpoint_mgr.state.search_after.clone(),
            )?;

            let mut destination: Box<dyn Destination> = match dest.as_str() {
                "stdout" => Box::new(StdoutDestination),
                "openobserve" | "oo" => {
                    let url = dest_url
                        .ok_or_else(|| anyhow::anyhow!("--dest-url required for openobserve"))?;
                    let stream = dest_stream
                        .ok_or_else(|| anyhow::anyhow!("--dest-stream required for openobserve"))?;
                    let username = dest_username.ok_or_else(|| {
                        anyhow::anyhow!("--dest-username required for openobserve")
                    })?;
                    let password = dest_password.ok_or_else(|| {
                        anyhow::anyhow!("--dest-password required for openobserve")
                    })?;
                    Box::new(OpenObserveDestination::new(
                        url,
                        dest_org,
                        stream,
                        username,
                        password,
                        OpenObserveOptions::default(),
                    )?)
                }
                other => anyhow::bail!(
                    "Unknown destination '{}'. Use 'stdout' or 'openobserve'",
                    other
                ),
            };

            let transformer = Transformer::identity();

            run_extraction(
                &mut source,
                &mut *destination,
                &transformer,
                &mut checkpoint_mgr,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_from_config(cfg: EsiftConfig) -> Result<()> {
    let query: serde_json::Value = serde_json::from_str(&cfg.source.query)?;

    let auth = resolve_auth(
        cfg.source.auth_type.as_deref(),
        cfg.source.username,
        cfg.source.password,
        cfg.source.aws_region,
    )
    .await?;

    let mut checkpoint_mgr = CheckpointManager::new(cfg.checkpoint_path)?;

    let mut source = OpenSearchSource::new(
        &cfg.source.url,
        &cfg.source.index,
        query,
        cfg.source.batch_size,
        auth,
        checkpoint_mgr.state.search_after.clone(),
    )?;

    let mut destination: Box<dyn Destination> = match cfg.destination {
        DestConfig::Stdout => Box::new(StdoutDestination),
        DestConfig::OpenObserve {
            url,
            org,
            stream,
            username,
            password,
            options,
        } => Box::new(OpenObserveDestination::new(
            url, org, stream, username, password, *options,
        )?),
    };

    let transformer = Transformer::identity();

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
                        checkpoint.state.record_batch(written, source.cursor());
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

// `aws_region` is only read on the aws-sigv4 path, which is compiled out when
// the `aws` feature is off; silence the unused-variable warning in that build.
#[cfg_attr(not(feature = "aws"), allow(unused_variables))]
async fn resolve_auth(
    auth_type: Option<&str>,
    username: Option<String>,
    password: Option<String>,
    aws_region: Option<String>,
) -> Result<Auth> {
    match auth_type {
        Some("aws-sigv4") => {
            #[cfg(feature = "aws")]
            {
                use aws_config::BehaviorVersion;
                let mut builder = aws_config::defaults(BehaviorVersion::latest());

                let region = if let Some(r) = aws_region {
                    Some(aws_config::Region::new(r))
                } else if let Ok(r) = std::env::var("AWS_REGION") {
                    Some(aws_config::Region::new(r))
                } else if let Ok(r) = std::env::var("AWS_DEFAULT_REGION") {
                    Some(aws_config::Region::new(r))
                } else {
                    None
                };

                if let Some(r) = region {
                    builder = builder.region(r);
                }

                let sdk_config = builder.load().await;
                let provider = sdk_config
                    .credentials_provider()
                    .ok_or_else(|| anyhow::anyhow!("No AWS credentials provider found. Please verify your environment variables or ~/.aws/credentials."))?;

                let resolved_region = sdk_config
                    .region()
                    .map(|r| r.as_ref().to_string())
                    .ok_or_else(|| anyhow::anyhow!("AWS region is required for aws-sigv4. Specify it via --source-aws-region, config file, or AWS_REGION environment variable."))?;

                Ok(Auth::AwsSigV4 {
                    region: resolved_region,
                    provider: provider.clone(),
                })
            }
            #[cfg(not(feature = "aws"))]
            {
                anyhow::bail!("AWS SigV4 authentication is not enabled. Recompile esift with the 'aws' feature enabled to use this feature.")
            }
        }
        Some("basic") => {
            let u =
                username.ok_or_else(|| anyhow::anyhow!("Username is required for basic auth"))?;
            Ok(Auth::Basic {
                username: u,
                password,
            })
        }
        Some("none") => Ok(Auth::None),
        Some(other) => {
            anyhow::bail!(
                "Unsupported source-auth-type '{}'. Use 'basic', 'aws-sigv4', or 'none'.",
                other
            )
        }
        None => {
            if let Some(u) = username {
                Ok(Auth::Basic {
                    username: u,
                    password,
                })
            } else {
                Ok(Auth::None)
            }
        }
    }
}
