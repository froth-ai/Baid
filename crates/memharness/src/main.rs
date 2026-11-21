//! `memharness`: benchmark harness. See the library docs; `just load` runs datagen and load.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use adapters::{BoltBackend, BoltConfig, Resilient, S3Backend, S3Config};
use anyhow::{bail, Context};
use baid_core::Backend;
use clap::{Parser, Subcommand, ValueEnum};
use memharness::datagen::{self, Scale};
use memharness::dataset::Dataset;
use memharness::load::{load, LoadOptions};
use memharness::probe::{first_listening, DEFAULT_BAID_ADDRS};
use memharness::verify::{verify, VerifyOptions};

/// Deadline of one backend call, including `Resilient`'s retries.
const CALL_DEADLINE: Duration = Duration::from_secs(30);
/// Bound on one driver call inside that.
const DRIVER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(name = "memharness", about = "BAiD benchmark harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a synthetic dataset into <out>/<scale>-s<seed>-v<generator>/.
    Datagen {
        /// Size preset.
        #[arg(long, value_enum, default_value = "small")]
        scale: Scale,
        /// Seed; the same scale and seed always give the same bytes.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Directory that holds the dataset directories.
        #[arg(long, default_value = "bench/datasets")]
        out: PathBuf,
    },
    /// Load a dataset into backends, straight through the adapters (BAiD must be stopped).
    Load(Box<LoadArgs>),
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Target {
    /// Neo4j (or any Bolt server).
    Bolt,
    /// S3 or an S3-compatible store.
    S3,
}

#[derive(clap::Args)]
struct LoadArgs {
    /// The dataset directory (holding manifest.json and memories.jsonl).
    #[arg(long, required_unless_present = "scale", conflicts_with = "scale")]
    dataset: Option<PathBuf>,
    /// Or: the dataset's scale, to find it under --root by scale and seed.
    #[arg(long, value_enum)]
    scale: Option<Scale>,
    /// The dataset's seed (with --scale).
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Directory that holds the dataset directories (with --scale).
    #[arg(long, default_value = "bench/datasets")]
    root: PathBuf,
    /// Backends to load (repeatable).
    #[arg(long = "target", value_enum, required = true)]
    targets: Vec<Target>,
    /// After loading, check what each backend returns against the dataset.
    #[arg(long)]
    verify: bool,
    /// Only verify; load nothing.
    #[arg(long, conflicts_with = "verify")]
    verify_only: bool,
    /// Rows sampled per tenant by the verification.
    #[arg(long, default_value_t = 50)]
    samples: usize,
    /// Writes in flight per backend.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    /// Load even though a BAiD server answers locally. Only after its caches are flushed for
    /// the dataset's tenants: the load bypasses BAiD's write log.
    #[arg(long)]
    allow_running_baid: bool,
    /// Where BAiD would listen (repeatable); defaults to 127.0.0.1:7070 and :7071.
    #[arg(long = "baid-addr")]
    baid_addrs: Vec<SocketAddr>,
    /// Bolt URL.
    #[arg(long, env = "BAID_LOAD_NEO4J_URL")]
    neo4j_url: Option<String>,
    /// Bolt user.
    #[arg(long, env = "BAID_LOAD_NEO4J_USER", default_value = "neo4j")]
    neo4j_user: String,
    /// Bolt password.
    #[arg(long, env = "BAID_LOAD_NEO4J_PASSWORD", hide_env_values = true)]
    neo4j_password: Option<String>,
    /// S3 URL: s3://bucket/prefix?endpoint=http://host:port
    #[arg(long, env = "BAID_LOAD_S3_URL")]
    s3_url: Option<String>,
    /// S3 access key id.
    #[arg(long, env = "BAID_LOAD_S3_KEY", hide_env_values = true)]
    s3_key: Option<String>,
    /// S3 secret key.
    #[arg(long, env = "BAID_LOAD_S3_SECRET", hide_env_values = true)]
    s3_secret: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<bool> {
    match cli.cmd {
        Cmd::Datagen { scale, seed, out } => {
            let started = std::time::Instant::now();
            let (dir, m) = datagen::write_dataset(&out, scale, seed)
                .with_context(|| format!("writing the dataset under {}", out.display()))?;
            println!(
                "{}: {} tenants, {} memories, {} entities, {} bytes, blake3 {} ({:.1} s)",
                dir.display(),
                m.tenants.len(),
                m.memories,
                m.entities,
                m.bytes,
                m.blake3,
                started.elapsed().as_secs_f64()
            );
            Ok(true)
        }
        Cmd::Load(args) => load_cmd(*args).await,
    }
}

async fn load_cmd(a: LoadArgs) -> anyhow::Result<bool> {
    let dir = match (&a.dataset, a.scale) {
        (Some(d), _) => d.clone(),
        (None, Some(scale)) => a.root.join(datagen::dataset_name(scale, a.seed)),
        (None, None) => bail!("give --dataset or --scale"),
    };
    let ds = Dataset::open(&dir)?;
    let m = ds.manifest();
    println!(
        "dataset {}: {} tenants, {} memories (hash ok)",
        m.name,
        m.tenants.len(),
        m.memories
    );

    if !a.verify_only {
        let addrs: Vec<SocketAddr> = if a.baid_addrs.is_empty() {
            DEFAULT_BAID_ADDRS
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect()
        } else {
            a.baid_addrs.clone()
        };
        if let Some(addr) = first_listening(&addrs).await {
            if !a.allow_running_baid {
                bail!(
                    "something is listening on {addr} (a BAiD port). Loading writes straight \
                     to the backends and bypasses BAiD's write log, so a running BAiD could \
                     keep serving results cached before the load. Stop BAiD and re-run, or \
                     flush its caches for the dataset's tenants and pass --allow-running-baid"
                );
            }
            eprintln!(
                "warning: {addr} is listening; loading anyway (--allow-running-baid). Flush \
                 BAiD's caches for the dataset's tenants."
            );
        }
    }

    let mut all_ok = true;
    for target in &a.targets {
        let ok = match target {
            Target::Bolt => {
                let Some(url) = a.neo4j_url.clone() else {
                    bail!("--target bolt needs --neo4j-url (or BAID_LOAD_NEO4J_URL)");
                };
                let inner = BoltBackend::connect(BoltConfig {
                    url,
                    user: Some(a.neo4j_user.clone()),
                    password: a.neo4j_password.clone(),
                    max_connections: a.concurrency + 4,
                    call_timeout: DRIVER_TIMEOUT,
                })
                .await
                .context("connecting to Neo4j")?;
                let backend = Resilient::with_defaults(inner, CALL_DEADLINE);
                one_target(&backend, "bolt", &ds, &a, true).await?
            }
            Target::S3 => {
                let Some(url) = a.s3_url.clone() else {
                    bail!("--target s3 needs --s3-url (or BAID_LOAD_S3_URL)");
                };
                let inner = S3Backend::connect(S3Config {
                    url,
                    access_key: a.s3_key.clone(),
                    secret_key: a.s3_secret.clone(),
                    call_timeout: DRIVER_TIMEOUT,
                    scan_limit: 1_000,
                })
                .context("configuring S3")?;
                let backend = Resilient::with_defaults(inner, CALL_DEADLINE);
                // S3 has no text index: unfiltered search only scans the newest memories.
                one_target(&backend, "s3", &ds, &a, false).await?
            }
        };
        all_ok &= ok;
    }
    Ok(all_ok)
}

async fn one_target<B: Backend>(
    backend: &B,
    label: &str,
    ds: &Dataset,
    a: &LoadArgs,
    full_text: bool,
) -> anyhow::Result<bool> {
    if !a.verify_only {
        let opts = LoadOptions {
            concurrency: a.concurrency,
            ..LoadOptions::default()
        };
        let r = load(backend, ds, &opts, label).await?;
        println!(
            "{label}: loaded {} rows in {:.1} s ({:.0} rows/s, {} in flight)",
            r.written,
            r.elapsed.as_secs_f64(),
            r.rate(),
            a.concurrency
        );
    }
    if !(a.verify || a.verify_only) {
        return Ok(true);
    }
    let opts = VerifyOptions {
        samples_per_tenant: a.samples,
        full_text,
        ..VerifyOptions::default()
    };
    let r = verify(backend, ds, &opts).await?;
    println!(
        "{label}: verify {}\n{r}",
        if r.ok() { "ok" } else { "FAILED" }
    );
    Ok(r.ok())
}
