//! Configuration structs.
//!
//! The server binary layers built-in defaults, `baid.toml`, `BAID_*` environment variables
//! and command-line flags into a [`Config`], then calls [`Config::validate`] before it binds
//! a port. Secrets are never stored here: fields ending in `_env` name the environment
//! variable that holds the value.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::{AdapterId, TenantId};
use crate::units::{ByteSize, HumanDuration};

/// Hard cap on [`TenantConfig::stale_if_error`]: long enough to ride out a breaker cycle or a
/// backend failover; past it agents should see errors rather than ever older data (G23).
pub const MAX_STALE_IF_ERROR: Duration = Duration::from_secs(120);

/// The whole service configuration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Listening sockets.
    pub server: ServerConfig,
    /// Cache tiers.
    pub cache: CacheConfig,
    /// Metrics and traces.
    pub telemetry: TelemetryConfig,
    /// One entry per tenant.
    pub tenants: Vec<TenantConfig>,
}

/// Listening sockets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Public API (recall, remember, MCP).
    pub listen: SocketAddr,
    /// Admin API and `/metrics`. Keep it on loopback or a private network.
    pub admin_listen: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen: SocketAddr::from(([0, 0, 0, 0], 7070)),
            admin_listen: SocketAddr::from(([127, 0, 0, 1], 7071)),
        }
    }
}

/// Cache tiers. The NVMe tier is on only when `nvme_dir` is set.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// RAM tier size for the whole instance.
    pub ram_bytes: ByteSize,
    /// Directory for the NVMe tier; omit to turn the disk tier off.
    pub nvme_dir: Option<PathBuf>,
    /// NVMe tier size.
    pub nvme_bytes: ByteSize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            ram_bytes: ByteSize::gib(2),
            nvme_dir: None,
            nvme_bytes: ByteSize::gib(20),
        }
    }
}

/// Metrics and traces.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    /// OTLP gRPC endpoint for traces; omit to turn tracing export off.
    pub otlp_endpoint: Option<String>,
}

/// The kinds of memory backend BAiD can sit in front of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AdapterKind {
    /// Neo4j or Memgraph over Bolt.
    Bolt,
    /// S3 or any S3-compatible object store (SeaweedFS, MinIO, Ceph).
    S3,
    /// Zep (after launch).
    Zep,
    /// Mem0 (after launch).
    Mem0,
    /// In-process and non-durable: data is lost on restart. For development, demos and
    /// smoke tests. `adapter_url` is ignored (use `memory://`).
    Memory,
}

impl AdapterKind {
    /// Lower-case name; also the [`AdapterId`] used in cache keys.
    pub fn as_str(self) -> &'static str {
        match self {
            AdapterKind::Bolt => "bolt",
            AdapterKind::S3 => "s3",
            AdapterKind::Zep => "zep",
            AdapterKind::Mem0 => "mem0",
            AdapterKind::Memory => "memory",
        }
    }

    /// The adapter id for this kind.
    pub fn adapter_id(self) -> AdapterId {
        // The names above are fixed, valid slugs.
        match AdapterId::new(self.as_str()) {
            Ok(id) => id,
            Err(_) => unreachable!("adapter kind names are valid ids"),
        }
    }
}

/// One tenant: its key, its backend and its limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    /// Tenant id; the first segment of every key it owns.
    pub id: TenantId,
    /// Environment variable holding this tenant's API key.
    pub api_key_env: String,
    /// Backend kind.
    pub adapter: AdapterKind,
    /// Backend address, e.g. `bolt://neo4j:7687` or `s3://bucket/prefix`.
    pub adapter_url: String,
    /// Environment variable holding the backend user, if it needs one.
    #[serde(default)]
    pub adapter_user_env: Option<String>,
    /// Environment variable holding the backend password, if it needs one.
    #[serde(default)]
    pub adapter_password_env: Option<String>,
    /// Hard upper bound on how long an entry lives.
    #[serde(default = "defaults::ttl")]
    pub ttl: HumanDuration,
    /// How long past expiry an entry may still be served when the backend is failing
    /// (stale-if-error, G23). At most `ttl` and at most [`MAX_STALE_IF_ERROR`]; `0s`
    /// turns it off.
    #[serde(default = "defaults::stale_if_error")]
    pub stale_if_error: HumanDuration,
    /// Tokenizer used for token counts.
    #[serde(default = "defaults::tokenizer")]
    pub tokenizer: String,
    /// This tenant's share of the RAM tier.
    #[serde(default = "defaults::tenant_ram")]
    pub ram_bytes: ByteSize,
    /// Per-call deadline for backend requests.
    #[serde(default = "defaults::backend_timeout")]
    pub backend_timeout: HumanDuration,
    /// Speculative prefetch settings.
    #[serde(default)]
    pub prefetch: PrefetchConfig,
}

/// Speculative prefetch settings for one tenant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrefetchConfig {
    /// Master switch.
    pub enabled: bool,
    /// Candidates prefetched per recall at most.
    pub k: usize,
    /// Candidates below this confidence are dropped.
    pub min_confidence: f32,
    /// Speculative backend calls per second (token bucket rate).
    pub calls_per_second: f64,
    /// Speculative backend calls in flight at once.
    pub max_concurrent: usize,
    /// Subtracted from the think-time estimate to get a prefetch deadline.
    pub safety_margin: HumanDuration,
    /// A prefetch is not started if less than this remains after the margin.
    pub min_useful_window: HumanDuration,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        PrefetchConfig {
            enabled: true,
            k: 4,
            min_confidence: 0.35,
            calls_per_second: 20.0,
            max_concurrent: 4,
            safety_margin: HumanDuration::millis(150),
            min_useful_window: HumanDuration::millis(50),
        }
    }
}

mod defaults {
    use super::*;
    pub fn ttl() -> HumanDuration {
        HumanDuration::secs(300)
    }
    pub fn stale_if_error() -> HumanDuration {
        HumanDuration::secs(30)
    }
    pub fn tokenizer() -> String {
        "cl100k_base".into()
    }
    pub fn tenant_ram() -> ByteSize {
        ByteSize::mib(256)
    }
    pub fn backend_timeout() -> HumanDuration {
        HumanDuration::secs(2)
    }
}

/// One problem found by [`Config::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigIssue {
    /// Dotted path of the offending field, e.g. `tenants[acme].prefetch.k`.
    pub field: String,
    /// What is wrong.
    pub reason: String,
}

/// Every problem in a config. The server refuses to start (or keeps the previous config on
/// reload) when this is returned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct ConfigError(pub Vec<ConfigIssue>);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid config:")?;
        for i in &self.0 {
            write!(f, "\n  {}: {}", i.field, i.reason)?;
        }
        Ok(())
    }
}

/// Collects issues while walking the config.
struct Issues(Vec<ConfigIssue>);

impl Issues {
    fn check(&mut self, ok: bool, field: impl Into<String>, reason: impl Into<String>) {
        if !ok {
            self.0.push(ConfigIssue {
                field: field.into(),
                reason: reason.into(),
            });
        }
    }
}

fn is_env_name(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with(|c: char| c.is_ascii_digit())
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

impl Config {
    /// Checks ranges and cross-field rules. Does not read the environment; resolving the
    /// `*_env` references is the server's job.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut is = Issues(Vec::new());

        is.check(
            self.server.listen != self.server.admin_listen,
            "server.admin_listen",
            "must differ from server.listen",
        );
        is.check(
            self.cache.ram_bytes >= ByteSize::mib(16),
            "cache.ram_bytes",
            "must be at least 16MiB",
        );
        if self.cache.nvme_dir.is_some() {
            is.check(
                self.cache.nvme_bytes >= ByteSize::mib(64),
                "cache.nvme_bytes",
                "must be at least 64MiB when the NVMe tier is on",
            );
        }

        is.check(
            !self.tenants.is_empty(),
            "tenants",
            "at least one tenant is required",
        );
        let mut ids = HashSet::new();
        let mut key_envs = HashSet::new();
        for t in &self.tenants {
            let p = format!("tenants[{}]", t.id);
            is.check(ids.insert(&t.id), &p, "duplicate tenant id");
            is.check(
                is_env_name(&t.api_key_env),
                format!("{p}.api_key_env"),
                "must be an environment variable name (A-Z, 0-9, _)",
            );
            is.check(
                key_envs.insert(&t.api_key_env),
                format!("{p}.api_key_env"),
                "two tenants share one API key variable; tenants must have their own key",
            );
            for (name, v) in [
                ("adapter_user_env", &t.adapter_user_env),
                ("adapter_password_env", &t.adapter_password_env),
            ] {
                if let Some(v) = v {
                    is.check(
                        is_env_name(v),
                        format!("{p}.{name}"),
                        "must be an environment variable name (A-Z, 0-9, _)",
                    );
                }
            }
            is.check(
                !t.adapter_url.trim().is_empty(),
                format!("{p}.adapter_url"),
                "must not be empty",
            );
            is.check(
                t.ttl.get() >= Duration::from_secs(1) && t.ttl.get() <= Duration::from_secs(86_400),
                format!("{p}.ttl"),
                "must be between 1s and 24h",
            );
            is.check(
                t.stale_if_error.get() <= t.ttl.get().min(MAX_STALE_IF_ERROR),
                format!("{p}.stale_if_error"),
                "must not exceed ttl or 120s",
            );
            is.check(
                t.backend_timeout.get() >= Duration::from_millis(10)
                    && t.backend_timeout.get() <= Duration::from_secs(60),
                format!("{p}.backend_timeout"),
                "must be between 10ms and 60s",
            );
            is.check(
                t.ram_bytes >= ByteSize::mib(1),
                format!("{p}.ram_bytes"),
                "must be at least 1MiB",
            );

            let pf = &t.prefetch;
            let pp = format!("{p}.prefetch");
            is.check(
                (1..=32).contains(&pf.k),
                format!("{pp}.k"),
                "must be 1 to 32",
            );
            is.check(
                (0.0..=1.0).contains(&pf.min_confidence),
                format!("{pp}.min_confidence"),
                "must be 0.0 to 1.0",
            );
            is.check(
                pf.calls_per_second.is_finite() && pf.calls_per_second > 0.0,
                format!("{pp}.calls_per_second"),
                "must be greater than 0",
            );
            is.check(
                (1..=256).contains(&pf.max_concurrent),
                format!("{pp}.max_concurrent"),
                "must be 1 to 256",
            );
            is.check(
                pf.safety_margin.get() <= Duration::from_secs(10),
                format!("{pp}.safety_margin"),
                "must be at most 10s",
            );
        }
        // Each tenant gets its own RAM tier of `ram_bytes`, so the instance really uses the
        // sum. Refuse configs that would exceed the instance budget.
        let total: u64 = self
            .tenants
            .iter()
            .fold(0u64, |acc, t| acc.saturating_add(t.ram_bytes.bytes()));
        is.check(
            total <= self.cache.ram_bytes.bytes(),
            "tenants[].ram_bytes",
            format!(
                "tenants need {} in total but cache.ram_bytes is {}",
                ByteSize(total),
                self.cache.ram_bytes
            ),
        );

        if is.0.is_empty() {
            Ok(())
        } else {
            Err(ConfigError(is.0))
        }
    }

    /// The tenant with this id, if configured.
    pub fn tenant(&self, id: &TenantId) -> Option<&TenantConfig> {
        self.tenants.iter().find(|t| &t.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example from the Code architecture tab, section 4.2.
    const EXAMPLE: &str = r#"
[server]
listen = "0.0.0.0:7070"
admin_listen = "127.0.0.1:7071"

[cache]
ram_bytes = "2GiB"
nvme_dir = "/var/lib/baid"
nvme_bytes = "20GiB"

[telemetry]
otlp_endpoint = "http://otel-collector:4317"

[[tenants]]
id = "acme"
api_key_env = "BAID_KEY_ACME"
adapter = "bolt"
adapter_url = "bolt://neo4j:7687"
adapter_user_env = "NEO4J_USER"
adapter_password_env = "NEO4J_PASSWORD"
ttl = "5m"
stale_if_error = "30s"
tokenizer = "cl100k_base"

[tenants.prefetch]
enabled = true
k = 4
min_confidence = 0.35
calls_per_second = 20
max_concurrent = 4
safety_margin = "150ms"
"#;

    fn parse(s: &str) -> Config {
        toml::from_str(s).unwrap()
    }

    fn fields(c: &Config) -> Vec<String> {
        match c.validate() {
            Ok(()) => vec![],
            Err(ConfigError(is)) => is.into_iter().map(|i| i.field).collect(),
        }
    }

    #[test]
    fn example_parses_and_validates() {
        let c = parse(EXAMPLE);
        assert_eq!(c.validate(), Ok(()));
        let t = &c.tenants[0];
        assert_eq!(t.adapter, AdapterKind::Bolt);
        assert_eq!(t.ttl.get(), Duration::from_secs(300));
        assert_eq!(t.prefetch.safety_margin.get(), Duration::from_millis(150));
        assert_eq!(t.ram_bytes, ByteSize::mib(256), "default applied");
        assert_eq!(c.cache.ram_bytes, ByteSize::gib(2));
    }

    #[test]
    fn minimal_tenant_gets_defaults() {
        let c = parse(
            r#"
[[tenants]]
id = "solo"
api_key_env = "BAID_KEY"
adapter = "s3"
adapter_url = "s3://memories"
"#,
        );
        assert_eq!(c.validate(), Ok(()));
        assert_eq!(c.server, ServerConfig::default());
        assert_eq!(c.tenants[0].prefetch, PrefetchConfig::default());
    }

    #[test]
    fn typos_are_rejected() {
        let r: Result<Config, _> = toml::from_str("[server]\nlisten_addr = \"0.0.0.0:1\"\n");
        assert!(r.is_err());
    }

    #[test]
    fn bad_tenant_id_is_rejected_at_parse() {
        let r: Result<Config, _> = toml::from_str(
            "[[tenants]]\nid = \"a:b\"\napi_key_env = \"K\"\nadapter = \"bolt\"\nadapter_url = \"x\"\n",
        );
        assert!(r.is_err());
    }

    #[test]
    fn every_issue_is_reported() {
        let mut c = parse(EXAMPLE);
        c.server.admin_listen = c.server.listen;
        let mut t2 = c.tenants[0].clone();
        t2.prefetch.k = 0;
        t2.prefetch.min_confidence = 1.5;
        t2.stale_if_error = HumanDuration::secs(600);
        c.tenants.push(t2);
        let f = fields(&c);
        for expected in [
            "server.admin_listen",
            "tenants[acme]",
            "tenants[acme].api_key_env",
            "tenants[acme].prefetch.k",
            "tenants[acme].prefetch.min_confidence",
            "tenants[acme].stale_if_error",
        ] {
            assert!(
                f.iter().any(|x| x == expected),
                "missing {expected} in {f:?}"
            );
        }
    }

    #[test]
    fn stale_if_error_is_capped_by_ttl_and_120s() {
        let mut c = parse(EXAMPLE);
        c.tenants[0].ttl = HumanDuration::secs(600);
        c.tenants[0].stale_if_error = HumanDuration::secs(120);
        assert!(fields(&c).is_empty(), "120s under a 600s ttl is fine");
        c.tenants[0].stale_if_error = HumanDuration::secs(121);
        assert_eq!(fields(&c), ["tenants[acme].stale_if_error"], "over the cap");
        c.tenants[0].ttl = HumanDuration::secs(60);
        c.tenants[0].stale_if_error = HumanDuration::secs(61);
        assert_eq!(fields(&c), ["tenants[acme].stale_if_error"], "over the ttl");
        c.tenants[0].stale_if_error = HumanDuration::secs(0);
        assert!(fields(&c).is_empty(), "0 turns it off");
    }

    #[test]
    fn no_tenants_is_an_error() {
        assert_eq!(fields(&Config::default()), ["tenants"]);
    }

    #[test]
    fn tenant_ram_must_fit_the_instance() {
        let mut c = parse(EXAMPLE);
        c.cache.ram_bytes = ByteSize::mib(300);
        let mut t2 = c.tenants[0].clone();
        t2.id = TenantId::new("globex").unwrap();
        t2.api_key_env = "BAID_KEY_GLOBEX".into();
        c.tenants.push(t2); // 2 x 256MiB > 300MiB
        assert_eq!(fields(&c), ["tenants[].ram_bytes"]);
    }

    #[test]
    fn env_names_are_checked() {
        let mut c = parse(EXAMPLE);
        c.tenants[0].api_key_env = "my key".into();
        assert_eq!(fields(&c), ["tenants[acme].api_key_env"]);
    }
}
