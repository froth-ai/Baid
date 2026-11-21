//! One backend per tenant (Architecture section 4: "Adapter registry").

use std::collections::HashMap;
use std::sync::Arc;

use baid_core::{AdapterKind, Backend, BackendError, Config, MemoryBackend, TenantId};

use crate::bolt::{BoltBackend, BoltConfig};
use crate::resilient::{BreakerState, Resilient};
use crate::s3::{S3Backend, S3Config};

/// Memories an unfiltered S3 search scans (newest first).
pub const S3_SCAN_LIMIT: usize = 1_000;

/// A tenant's adapter could not be built.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The adapter kind exists in config but is not implemented yet.
    #[error("tenant {tenant}: adapter {kind:?} is not available in this build yet")]
    Unsupported {
        /// The tenant.
        tenant: TenantId,
        /// The requested kind.
        kind: AdapterKind,
    },
    /// The adapter refused its settings (for example a malformed URL).
    #[error("tenant {tenant}: {source}")]
    Invalid {
        /// The tenant.
        tenant: TenantId,
        /// Why.
        source: BackendError,
    },
}

/// Backend credentials, already read from the environment.
#[derive(Clone, Default)]
pub struct Credentials {
    /// User name, if the backend needs one.
    pub user: Option<String>,
    /// Password, if the backend needs one.
    pub password: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("user", &self.user.as_ref().map(|_| "***"))
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

/// Something that can report a circuit breaker's state cheaply (no I/O), for `/readyz`.
pub trait BreakerProbe: Send + Sync + 'static {
    /// The breaker's current state.
    fn breaker_state(&self) -> BreakerState;
}

impl<B: Backend> BreakerProbe for Resilient<B> {
    fn breaker_state(&self) -> BreakerState {
        self.state()
    }
}

/// Tenant → backend.
#[derive(Default)]
pub struct Registry {
    backends: HashMap<TenantId, Arc<dyn Backend>>,
    breakers: HashMap<TenantId, Arc<dyn BreakerProbe>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds one backend per configured tenant, each wrapped with retries and a circuit
    /// breaker. `credentials` supplies each tenant's resolved secrets. Connections are lazy,
    /// so this succeeds even when a backend is down.
    pub async fn from_config(
        config: &Config,
        credentials: impl Fn(&TenantId) -> Credentials,
    ) -> Result<Self, BuildError> {
        let mut r = Registry::new();
        for t in &config.tenants {
            let deadline = t.backend_timeout.get();
            match t.adapter {
                AdapterKind::Memory => {
                    r.insert_resilient(
                        t.id.clone(),
                        Arc::new(Resilient::with_defaults(MemoryBackend::new(), deadline)),
                    );
                }
                AdapterKind::Bolt => {
                    let creds = credentials(&t.id);
                    let bolt = BoltBackend::connect(BoltConfig {
                        url: t.adapter_url.clone(),
                        user: creds.user,
                        password: creds.password,
                        max_connections: 16,
                        call_timeout: deadline,
                    })
                    .await
                    .map_err(|source| BuildError::Invalid {
                        tenant: t.id.clone(),
                        source,
                    })?;
                    r.insert_resilient(
                        t.id.clone(),
                        Arc::new(Resilient::with_defaults(bolt, deadline)),
                    );
                }
                AdapterKind::S3 => {
                    let creds = credentials(&t.id);
                    let s3 = S3Backend::connect(S3Config {
                        url: t.adapter_url.clone(),
                        access_key: creds.user,
                        secret_key: creds.password,
                        call_timeout: deadline,
                        scan_limit: S3_SCAN_LIMIT,
                    })
                    .map_err(|source| BuildError::Invalid {
                        tenant: t.id.clone(),
                        source,
                    })?;
                    r.insert_resilient(
                        t.id.clone(),
                        Arc::new(Resilient::with_defaults(s3, deadline)),
                    );
                }
                kind => {
                    return Err(BuildError::Unsupported {
                        tenant: t.id.clone(),
                        kind,
                    })
                }
            }
        }
        Ok(r)
    }

    /// Sets (or replaces) a tenant's backend. It reports no breaker state.
    pub fn insert(&mut self, tenant: TenantId, backend: Arc<dyn Backend>) {
        self.breakers.remove(&tenant);
        self.backends.insert(tenant, backend);
    }

    /// Sets (or replaces) a tenant's backend wrapped in [`Resilient`], so its breaker state
    /// shows in [`Registry::breaker`].
    pub fn insert_resilient<B: Backend>(&mut self, tenant: TenantId, backend: Arc<Resilient<B>>) {
        self.breakers.insert(tenant.clone(), backend.clone());
        self.backends.insert(tenant, backend);
    }

    /// The tenant's breaker state, if its backend has one.
    pub fn breaker(&self, tenant: &TenantId) -> Option<BreakerState> {
        self.breakers.get(tenant).map(|b| b.breaker_state())
    }

    /// Every tenant with a backend.
    pub fn tenants(&self) -> impl Iterator<Item = &TenantId> {
        self.backends.keys()
    }

    /// The tenant's backend.
    pub fn get(&self, tenant: &TenantId) -> Option<Arc<dyn Backend>> {
        self.backends.get(tenant).cloned()
    }

    /// Number of tenants with a backend.
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// True if no tenant has a backend.
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ids: Vec<String> = self
            .backends
            .iter()
            .map(|(t, b)| format!("{t}={}", b.id()))
            .collect();
        ids.sort();
        write!(f, "Registry({})", ids.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(adapter: &str, url: &str) -> Config {
        toml::from_str(&format!(
            "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"{adapter}\"\nadapter_url = \"{url}\"\n"
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn builds_memory_and_bolt_without_connecting() {
        let acme = TenantId::new("acme").unwrap();
        let r = Registry::from_config(&config("memory", "memory://"), |_| Credentials::default())
            .await
            .unwrap();
        assert_eq!(r.get(&acme).unwrap().id().as_str(), "memory");
        assert!(r.get(&TenantId::new("other").unwrap()).is_none());

        // Nothing listens on this port; the pool is lazy, so building still succeeds.
        let r = Registry::from_config(&config("bolt", "bolt://127.0.0.1:1"), |_| {
            Credentials::default()
        })
        .await
        .unwrap();
        assert_eq!(format!("{r:?}"), "Registry(acme=bolt)");
    }

    #[tokio::test]
    async fn bad_urls_and_unbuilt_adapters_fail_loudly() {
        let e = Registry::from_config(&config("bolt", "http://x"), |_| Credentials::default())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("bolt://"), "{e}");
        let e = Registry::from_config(&config("s3", "http://b"), |_| Credentials::default())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("s3://"), "{e}");
        let e = Registry::from_config(&config("zep", "https://z"), |_| Credentials::default())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("Zep"), "{e}");
    }

    #[test]
    fn credentials_never_print() {
        let c = Credentials {
            user: Some("neo4j".into()),
            password: Some("hunter2".into()),
        };
        assert!(!format!("{c:?}").contains("hunter2"));
    }
}
