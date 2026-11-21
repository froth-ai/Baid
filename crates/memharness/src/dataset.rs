//! Dataset files: `bench/datasets/<name>/manifest.json` and `memories.jsonl` (G28).
//!
//! The JSONL holds one [`Row`] per line, `\n`-terminated. The manifest records how the file
//! was generated and its blake3 hash. Only manifests are committed; the JSONL is regenerated
//! from the seed, and [`Dataset::open`] refuses a JSONL whose hash does not match.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use baid_core::{EntityId, Fact, TenantId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::datagen::Scale;

/// File name of the manifest inside a dataset directory.
pub const MANIFEST: &str = "manifest.json";
/// File name of the rows inside a dataset directory.
pub const ROWS: &str = "memories.jsonl";

/// One memory of the dataset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// Position in the dataset, unique across all tenants.
    pub n: u64,
    /// The tenant the memory belongs to.
    pub tenant: TenantId,
    /// A letters-only word that appears in this memory's text and nowhere else in the
    /// dataset, so a search for it must return exactly this memory.
    pub unique: String,
    /// The memory text.
    pub text: String,
    /// Entities the memory mentions; the first is always the account.
    pub entities: Vec<EntityId>,
    /// One label: the kind of memory.
    pub labels: Vec<String>,
    /// When the fact became true.
    #[serde(with = "time::serde::rfc3339")]
    pub as_of: OffsetDateTime,
}

impl Row {
    /// The idempotency key the loaders use, so a re-run or resumed load is applied once.
    pub fn idempotency_key(&self, dataset: &str) -> String {
        format!("load:{dataset}:{}", self.n)
    }

    /// The fact the loaders write.
    pub fn fact(&self, dataset: &str) -> Fact {
        Fact {
            text: self.text.clone(),
            entities: self.entities.clone(),
            labels: self.labels.clone(),
            as_of: Some(self.as_of),
            idempotency_key: Some(self.idempotency_key(dataset)),
        }
    }
}

/// What a dataset is and how to rebuild it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Layout of the JSONL rows.
    pub format: u32,
    /// Version of the generator that wrote it; bumped whenever the output changes.
    pub generator: u32,
    /// Dataset name, `<scale>-s<seed>-v<generator>`; also the prefix of its tenant ids.
    pub name: String,
    /// Size preset.
    pub scale: Scale,
    /// Seed.
    pub seed: u64,
    /// Tenant ids, in file order.
    pub tenants: Vec<TenantId>,
    /// Memories per tenant.
    pub memories_per_tenant: u64,
    /// Memories in total.
    pub memories: u64,
    /// Distinct `(tenant, entity)` pairs mentioned.
    pub entities: u64,
    /// Size of the JSONL in bytes.
    pub bytes: u64,
    /// blake3 of the JSONL, hex.
    pub blake3: String,
}

impl Manifest {
    /// The command that regenerates this dataset into `root` (the directory holding the
    /// dataset directories).
    pub fn regenerate_command(&self, root: &Path) -> String {
        format!(
            "memharness datagen --scale {} --seed {} --out {}",
            self.scale.as_str(),
            self.seed,
            root.display()
        )
    }

    /// Which tenant row `n` belongs to (rows are written tenant by tenant).
    pub fn tenant_of(&self, n: u64) -> Option<&TenantId> {
        let i = n.checked_div(self.memories_per_tenant)?;
        self.tenants.get(usize::try_from(i).ok()?)
    }
}

/// Why a dataset cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    /// Reading a file failed.
    #[error("{path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// The manifest is not valid JSON of the expected shape.
    #[error("{path}: not a dataset manifest: {reason}")]
    BadManifest {
        /// The file.
        path: PathBuf,
        /// What was wrong.
        reason: String,
    },
    /// The JSONL is missing; it is not committed and must be generated.
    #[error("{path} is missing; generate it with `{regenerate}`")]
    Missing {
        /// The JSONL path.
        path: PathBuf,
        /// The command that regenerates it.
        regenerate: String,
    },
    /// The JSONL does not match the manifest's hash or size.
    #[error(
        "{path} does not match its manifest (blake3 {actual}, {actual_bytes} bytes; manifest \
         says {expected}, {expected_bytes} bytes); refusing to load. Regenerate it with \
         `{regenerate}`"
    )]
    Mismatch {
        /// The JSONL path.
        path: PathBuf,
        /// Hash the manifest records.
        expected: String,
        /// Hash of the file.
        actual: String,
        /// Size the manifest records.
        expected_bytes: u64,
        /// Size of the file.
        actual_bytes: u64,
        /// The command that regenerates it.
        regenerate: String,
    },
    /// A line of the JSONL is not a valid row.
    #[error("{path}:{line}: bad row: {reason}")]
    BadRow {
        /// The JSONL path.
        path: PathBuf,
        /// 1-based line number.
        line: u64,
        /// What was wrong.
        reason: String,
    },
}

/// A dataset whose JSONL matched its manifest when opened.
#[derive(Clone, Debug)]
pub struct Dataset {
    dir: PathBuf,
    manifest: Manifest,
}

impl Dataset {
    /// Reads the manifest and checks the JSONL's size and blake3 hash against it.
    pub fn open(dir: impl AsRef<Path>) -> Result<Dataset, DatasetError> {
        let dir = dir.as_ref().to_path_buf();
        let mpath = dir.join(MANIFEST);
        let text = std::fs::read_to_string(&mpath).map_err(|source| DatasetError::Io {
            path: mpath.clone(),
            source,
        })?;
        let manifest: Manifest =
            serde_json::from_str(&text).map_err(|e| DatasetError::BadManifest {
                path: mpath.clone(),
                reason: e.to_string(),
            })?;
        let root = dir.parent().unwrap_or(Path::new("."));
        let regenerate = manifest.regenerate_command(root);
        let rpath = dir.join(ROWS);
        let file = match File::open(&rpath) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(DatasetError::Missing {
                    path: rpath,
                    regenerate,
                })
            }
            Err(source) => {
                return Err(DatasetError::Io {
                    path: rpath,
                    source,
                })
            }
        };
        let (actual, actual_bytes) = hash_reader(file).map_err(|source| DatasetError::Io {
            path: rpath.clone(),
            source,
        })?;
        if actual != manifest.blake3 || actual_bytes != manifest.bytes {
            return Err(DatasetError::Mismatch {
                path: rpath,
                expected: manifest.blake3.clone(),
                actual,
                expected_bytes: manifest.bytes,
                actual_bytes,
                regenerate,
            });
        }
        Ok(Dataset { dir, manifest })
    }

    /// The manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The dataset's name.
    pub fn name(&self) -> &str {
        &self.manifest.name
    }

    /// The rows, in file order, read lazily.
    pub fn rows(&self) -> Result<impl Iterator<Item = Result<Row, DatasetError>>, DatasetError> {
        let path = self.dir.join(ROWS);
        let file = File::open(&path).map_err(|source| DatasetError::Io {
            path: path.clone(),
            source,
        })?;
        let mut line_no = 0u64;
        Ok(BufReader::new(file).lines().map(move |line| {
            line_no += 1;
            let line = line.map_err(|source| DatasetError::Io {
                path: path.clone(),
                source,
            })?;
            serde_json::from_str(&line).map_err(|e| DatasetError::BadRow {
                path: path.clone(),
                line: line_no,
                reason: e.to_string(),
            })
        }))
    }
}

/// blake3 (hex) and length of everything `r` yields.
pub fn hash_reader(mut r: impl Read) -> io::Result<(String, u64)> {
    let mut h = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        total += n as u64;
    }
    Ok((h.finalize().to_hex().to_string(), total))
}
