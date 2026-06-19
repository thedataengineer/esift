use anyhow::Result;
use clap::{Parser, Subcommand};
use esift_core::{
    checkpoint::CheckpointManager,
    config::{DestConfig, EsiftConfig, SourceConfig},
    dest::{
        file::FileDestination,
        openobserve::{OpenObserveDestination, OpenObserveOptions},
        s3::S3Destination,
        stdout::StdoutDestination,
        Destination,
    },
    source::{
        file::FileSource,
        opensearch::{Auth, OpenSearchSource},
        Source,
    },
};
use esift_transform::mapping::Transformer;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

mod metrics;
mod metrics_server;
mod secret;

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

        /// Parallel extraction slices (sliced PIT)
        #[arg(long, default_value = "1")]
        slices: usize,

        /// Serve Prometheus metrics on this address, e.g. 127.0.0.1:9090
        #[arg(long)]
        metrics_addr: Option<String>,
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
            slices,
            metrics_addr,
        } => {
            let query_value: serde_json::Value = serde_json::from_str(&query)?;

            let source_password = secret::resolve_opt(source_password)?;
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
            )?
            .with_slices(slices);

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
                    let password = secret::resolve(&password)?;
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
            let metrics = start_metrics(metrics_addr);

            run_extraction(
                &mut source,
                &mut *destination,
                &transformer,
                &mut checkpoint_mgr,
                &metrics,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_from_config(cfg: EsiftConfig) -> Result<()> {
    let mut checkpoint_mgr = CheckpointManager::new(cfg.checkpoint_path)?;

    let mut source = build_source(&cfg.source, checkpoint_mgr.state.search_after.clone()).await?;

    let mut destination: Box<dyn Destination> = match cfg.destination {
        DestConfig::Stdout => Box::new(StdoutDestination),
        DestConfig::File { path } => Box::new(FileDestination::new(path)?),
        DestConfig::S3 {
            bucket,
            prefix,
            region,
        } => Box::new(S3Destination::new(bucket, prefix, region)?),
        DestConfig::OpenObserve {
            url,
            org,
            stream,
            username,
            password,
            options,
        } => {
            let password = secret::resolve(&password)?;
            let mut options = *options;
            options.token = secret::resolve_opt(options.token)?;
            Box::new(OpenObserveDestination::new(
                url, org, stream, username, password, options,
            )?)
        }
    };

    let transformer = Transformer::new(cfg.transforms);
    let metrics = start_metrics(cfg.metrics_addr);

    run_extraction(
        &mut *source,
        &mut *destination,
        &transformer,
        &mut checkpoint_mgr,
        &metrics,
    )
    .await
}

/// Build the configured source (opensearch or file).
async fn build_source(
    cfg: &SourceConfig,
    resume_after: Option<Vec<serde_json::Value>>,
) -> Result<Box<dyn Source>> {
    match cfg.kind.as_str() {
        "file" => {
            let path = cfg
                .path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("source.path is required for the file source"))?;
            Ok(Box::new(FileSource::new(path)?))
        }
        "datadog-archive" => {
            use esift_core::source::datadog::archive::{Compression, DatadogArchiveSource};
            use esift_core::source::datadog::decompress::Codec;

            let bucket = cfg.dd_bucket.clone().ok_or_else(|| {
                anyhow::anyhow!("source.dd_bucket is required for the datadog-archive source")
            })?;
            let prefix = cfg.dd_prefix.clone().unwrap_or_default();
            let compression = match cfg.dd_compression.as_deref() {
                None | Some("auto") => Compression::Auto,
                Some("zstd") => Compression::Fixed(Codec::Zstd),
                Some("gzip") => Compression::Fixed(Codec::Gzip),
                Some(other) => {
                    anyhow::bail!("unknown dd_compression '{other}'. Use 'zstd', 'gzip', or 'auto'")
                }
            };
            Ok(Box::new(DatadogArchiveSource::new(
                bucket,
                prefix,
                cfg.dd_region.clone(),
                cfg.dd_from.clone(),
                cfg.dd_to.clone(),
                compression,
                resume_after,
            )?))
        }
        "datadog-api" => {
            use esift_core::source::datadog::api::DatadogApiSource;

            let api_key = secret::resolve_opt(cfg.dd_api_key.clone())?.ok_or_else(|| {
                anyhow::anyhow!("source.dd_api_key is required for the datadog-api source")
            })?;
            let app_key = secret::resolve_opt(cfg.dd_app_key.clone())?.ok_or_else(|| {
                anyhow::anyhow!("source.dd_app_key is required for the datadog-api source")
            })?;
            let site = cfg
                .dd_site
                .clone()
                .unwrap_or_else(|| "datadoghq.com".into());
            let query = cfg.dd_query.clone().unwrap_or_else(|| "*".into());
            let window_minutes = cfg.dd_window_minutes.unwrap_or(60);
            Ok(Box::new(DatadogApiSource::new(
                site,
                api_key,
                app_key,
                query,
                cfg.dd_from.clone(),
                cfg.dd_to.clone(),
                window_minutes,
                resume_after,
            )?))
        }
        "opensearch" | "" => {
            if cfg.url.is_empty() || cfg.index.is_empty() {
                anyhow::bail!("source.url and source.index are required for the opensearch source");
            }
            let query: serde_json::Value = serde_json::from_str(&cfg.query)?;
            let password = secret::resolve_opt(cfg.password.clone())?;
            let auth = resolve_auth(
                cfg.auth_type.as_deref(),
                cfg.username.clone(),
                password,
                cfg.aws_region.clone(),
            )
            .await?;
            let source = OpenSearchSource::new(
                &cfg.url,
                &cfg.index,
                query,
                cfg.batch_size,
                auth,
                resume_after,
            )?
            .with_slices(cfg.slices);
            Ok(Box::new(source))
        }
        other => anyhow::bail!("unknown source kind '{}'", other),
    }
}

/// Build the shared metrics handle and, when an address is set, start the
/// metrics endpoint in the background.
fn start_metrics(addr: Option<String>) -> metrics::SharedMetrics {
    let handle: metrics::SharedMetrics = Arc::new(metrics::Metrics::default());
    if let Some(addr) = addr {
        let m = handle.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics_server::serve(addr, m).await {
                error!("metrics server error: {e}");
            }
        });
    }
    handle
}

async fn run_extraction(
    source: &mut dyn Source,
    dest: &mut dyn Destination,
    transformer: &Transformer,
    checkpoint: &mut CheckpointManager,
    metrics: &metrics::SharedMetrics,
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
                        metrics.record_batch(written as u64);
                        progress.set_message(format!("{} documents extracted", total));
                    }
                    Err(e) => {
                        error!("Write failed: {}", e);
                        metrics.record_error();
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
                metrics.record_error();
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
