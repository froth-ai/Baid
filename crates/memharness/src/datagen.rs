//! Synthetic agent memory: accounts, the people, projects and tickets around them, and a few
//! hub entities (products, teams) that many accounts share.
//!
//! # Shape
//!
//! Each tenant has `accounts` accounts. Each account has 3-6 people, 1-3 projects and tickets
//! that open over time. Account activity follows a power law (a few hot accounts, a long
//! tail), and so does product popularity, which makes products hubs. Every memory mentions
//! its account plus up to three of: one of that account's people, projects or tickets, a
//! product, a team. Related memories therefore share entities, which is what neighbourhood
//! prediction (T23) needs to find.
//!
//! Texts come from templates over fixed word lists, so searches for a topic match many
//! memories. Each memory also ends with `Ref <unique>`: a letters-only word (`qz` and six
//! letters, derived from the row number) that appears nowhere else in the dataset, so a
//! search for it must return exactly that memory. Letters only, so full-text analysers keep
//! it as one token.
//!
//! Tenants reuse the same entity names (`account:kalomi` exists in every tenant), so any
//! leak between tenants shows up in `memharness load --verify`.
//!
//! # Determinism (G27)
//!
//! Output is a pure function of scale and seed, byte for byte on every platform: SplitMix64
//! with integer-only sampling, no floats, no hash-map iteration order, `\n` line endings.
//! A test pins the hash of the `tiny` dataset; [`GENERATOR_VERSION`] goes up whenever the
//! output changes on purpose.

use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use baid_core::{EntityId, TenantId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::dataset::{Manifest, Row, MANIFEST, ROWS};
use crate::rng::{SplitMix64, Weighted};

/// Version of the generator's output; bump when the bytes change on purpose.
pub const GENERATOR_VERSION: u32 = 1;
/// Version of the row layout.
pub const FORMAT_VERSION: u32 = 1;

/// First `as_of`: 2025-05-01T00:00:00Z.
const EPOCH_START: i64 = 1_746_057_600;
/// `as_of` values spread over 548 days (about 18 months).
const SPAN_SECS: i64 = 548 * 86_400;

/// Size presets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Scale {
    /// 2 tenants x 100 memories: tests.
    Tiny,
    /// 2 tenants x 5,000 memories: `just load`, the nightly small run.
    Small,
    /// 4 tenants x 50,000 memories.
    Medium,
    /// 8 tenants x 250,000 memories: the full benchmark (T38).
    Large,
}

/// The numbers behind a [`Scale`].
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    /// Tenants.
    pub tenants: u32,
    /// Memories per tenant.
    pub memories: u64,
    /// Accounts per tenant.
    pub accounts: u32,
    /// Products (hubs) per tenant.
    pub products: u32,
    /// Teams (hubs) per tenant; at most `TEAMS.len()`.
    pub teams: u32,
}

impl Scale {
    /// The preset's numbers.
    pub fn shape(self) -> Shape {
        let (tenants, memories, accounts, products, teams) = match self {
            Scale::Tiny => (2, 100, 12, 6, 4),
            Scale::Small => (2, 5_000, 200, 24, 8),
            Scale::Medium => (4, 50_000, 2_000, 48, 12),
            Scale::Large => (8, 250_000, 10_000, 96, 16),
        };
        Shape {
            tenants,
            memories,
            accounts,
            products,
            teams,
        }
    }

    /// Lower-case name, as on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            Scale::Tiny => "tiny",
            Scale::Small => "small",
            Scale::Medium => "medium",
            Scale::Large => "large",
        }
    }
}

/// The dataset's name: `<scale>-s<seed>-v<generator version>`. It prefixes the tenant ids,
/// so a dataset from a changed generator never lands in the tenants (or reuses the
/// idempotency keys) of an older one.
pub fn dataset_name(scale: Scale, seed: u64) -> String {
    format!("{}-s{seed}-v{GENERATOR_VERSION}", scale.as_str())
}

// Word lists. Letters only, lower case, none starting with "qz" (reserved for unique words).
const SYLLABLES: [&str; 32] = [
    "ka", "lo", "mi", "ra", "ve", "to", "su", "ne", "di", "ba", "fo", "ge", "hu", "ji", "ko", "la",
    "me", "no", "pi", "ru", "sa", "te", "vo", "wi", "ze", "bo", "cu", "da", "fe", "gi", "ha", "mo",
];
const FIRST_NAMES: [&str; 32] = [
    "ada", "ben", "cara", "dana", "eli", "fay", "gus", "hana", "ivan", "jade", "kai", "lena",
    "milo", "nora", "omar", "pia", "quinn", "rosa", "sam", "tara", "uma", "vic", "wren", "xena",
    "yuri", "zoe", "alma", "bruno", "cleo", "dev", "esme", "finn",
];
const PROJECTS: [&str; 24] = [
    "atlas", "beacon", "cobalt", "delta", "ember", "falcon", "granite", "harbor", "indigo",
    "juniper", "keystone", "lantern", "meridian", "nimbus", "orchid", "pioneer", "quartz",
    "redwood", "summit", "tundra", "unity", "vertex", "willow", "zenith",
];
/// Team names; also the most teams a tenant can have.
pub const TEAMS: [&str; 16] = [
    "billing",
    "platform",
    "support",
    "sales",
    "legal",
    "data",
    "mobile",
    "infra",
    "growth",
    "design",
    "finance",
    "research",
    "success",
    "security",
    "operations",
    "identity",
];
/// Topic words, most popular first (sampled with a power law).
pub const TOPICS: [&str; 40] = [
    "renewal",
    "pricing",
    "outage",
    "migration",
    "invoice",
    "onboarding",
    "security",
    "audit",
    "latency",
    "integration",
    "contract",
    "roadmap",
    "budget",
    "escalation",
    "training",
    "deployment",
    "backup",
    "compliance",
    "discount",
    "feedback",
    "licensing",
    "upgrade",
    "reporting",
    "analytics",
    "permissions",
    "storage",
    "procurement",
    "localization",
    "accessibility",
    "forecast",
    "headcount",
    "partnership",
    "retention",
    "uptime",
    "billing",
    "sandbox",
    "webhooks",
    "encryption",
    "invoicing",
    "throttling",
];
const STATES: [&str; 5] = ["on track", "delayed", "blocked", "done", "under review"];
const VERBS: [&str; 5] = ["approve", "postpone", "expand", "cancel", "revisit"];
const PREFS: [&str; 5] = [
    "email updates",
    "weekly calls",
    "short summaries",
    "written reports",
    "quarterly reviews",
];
const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

/// Kinds of memory, with their weights; the kind is also the memory's label.
const KINDS: [(&str, u64); 6] = [
    ("status", 30),
    ("issue", 25),
    ("decision", 15),
    ("preference", 10),
    ("contact", 10),
    ("plan", 10),
];

/// Every fixed word the generator can put into a text (for tests).
pub fn vocabulary() -> impl Iterator<Item = &'static str> {
    SYLLABLES
        .iter()
        .chain(&FIRST_NAMES)
        .chain(&PROJECTS)
        .chain(&TEAMS)
        .chain(&TOPICS)
        .chain(&STATES)
        .chain(&VERBS)
        .chain(&PREFS)
        .chain(&MONTHS)
        .copied()
}

/// Letters of a unique word after the `qz` prefix.
const UNIQUE_LETTERS: u32 = 6;

/// The unique word of row `n`: `qz` and `n` in base 26 as six letters. Bijective for
/// `n < 26^6` (308 million).
pub fn unique_word(n: u64) -> String {
    assert!(n < 26u64.pow(UNIQUE_LETTERS), "row number too large");
    let mut letters = [b'a'; UNIQUE_LETTERS as usize];
    let mut x = n;
    for slot in letters.iter_mut().rev() {
        *slot = b'a' + (x % 26) as u8;
        x /= 26;
    }
    let mut s = String::from("qz");
    s.extend(letters.iter().map(|&b| char::from(b)));
    s
}

/// The row number a unique word encodes, if it is one.
pub fn decode_unique(word: &str) -> Option<u64> {
    let rest = word.strip_prefix("qz")?;
    if rest.len() != UNIQUE_LETTERS as usize {
        return None;
    }
    rest.bytes().try_fold(0u64, |acc, b| {
        b.is_ascii_lowercase()
            .then(|| acc * 26 + u64::from(b - b'a'))
    })
}

/// The unique word at the end of a generated memory text, if any.
pub fn unique_in_text(text: &str) -> Option<u64> {
    let last = text.rsplit("Ref ").next()?;
    decode_unique(last.trim_end_matches('.'))
}

/// A name built from syllables: a bijection from `0..32^syllables`.
fn name(mut i: u64, syllables: u32) -> String {
    let mut s = String::new();
    for _ in 0..syllables {
        s.push_str(SYLLABLES[(i % 32) as usize]);
        i /= 32;
    }
    s
}

fn capitalised(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
        None => String::new(),
    }
}

fn entity(s: String) -> EntityId {
    EntityId::new(s).expect("generated entity ids are valid")
}

/// A named entity: its id and how texts mention it.
#[derive(Clone)]
struct Named {
    id: EntityId,
    display: String,
}

struct Account {
    id: EntityId,
    display: String,
    people: Vec<Named>,
    projects: Vec<Named>,
    tickets: Vec<EntityId>,
    ticket_prefix: String,
}

struct World {
    accounts: Vec<Account>,
    products: Vec<Named>,
    teams: Vec<Named>,
    activity: Weighted,
    product_pop: Weighted,
    topics: Weighted,
    kinds: Weighted,
    next_ticket: u64,
}

impl World {
    fn new(shape: &Shape, rng: &mut SplitMix64) -> World {
        // Odd multiplier mod 32^3: a permutation, so names stay unique but do not start
        // "kakaka", "lokaka", ...
        let accounts = (0..u64::from(shape.accounts))
            .map(|i| {
                let n = name((i * 7_919 + 12_345) % 32_768, 3);
                let people = (0..rng.range(3, 6))
                    .map(|_| *rng.pick(&FIRST_NAMES))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .map(|first| Named {
                        id: entity(format!("person:{first}@{n}")),
                        display: capitalised(first),
                    })
                    .collect();
                let projects = (0..rng.range(1, 3))
                    .map(|_| *rng.pick(&PROJECTS))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .map(|p| Named {
                        id: entity(format!("project:{n}-{p}")),
                        display: format!("{} {}", capitalised(&n), capitalised(p)),
                    })
                    .collect();
                Account {
                    id: entity(format!("account:{n}")),
                    display: capitalised(&n),
                    people,
                    projects,
                    tickets: Vec::new(),
                    ticket_prefix: n[..3].to_ascii_uppercase(),
                }
            })
            .collect();
        let products = (0..u64::from(shape.products))
            .map(|i| {
                let n = name((i * 37 + 101) % 1_024, 2) + "ly";
                Named {
                    id: entity(format!("product:{n}")),
                    display: capitalised(&n),
                }
            })
            .collect();
        let teams = TEAMS[..shape.teams as usize]
            .iter()
            .map(|t| Named {
                id: entity(format!("team:{t}")),
                display: format!("{t} team"),
            })
            .collect();
        World {
            accounts,
            products,
            teams,
            activity: Weighted::zipf(shape.accounts as usize, 5),
            product_pop: Weighted::zipf(shape.products as usize, 10),
            topics: Weighted::zipf(TOPICS.len(), 20),
            kinds: Weighted::new(KINDS.iter().map(|(_, w)| *w)),
            next_ticket: 1_000,
        }
    }

    /// One memory: its text (without the `Ref` suffix), entities and label.
    fn memory(&mut self, rng: &mut SplitMix64) -> (String, Vec<EntityId>, &'static str) {
        let a = self.activity.sample(rng);
        let kind = KINDS[self.kinds.sample(rng)].0;
        let topic = TOPICS[self.topics.sample(rng)];
        let product = if rng.chance(1, 2) {
            Some(self.products[self.product_pop.sample(rng)].clone())
        } else {
            None
        };
        let team = rng.chance(2, 5).then(|| rng.pick(&self.teams).clone());
        let acct = &self.accounts[a];
        let person = rng.pick(&acct.people).clone();
        let project = rng.pick(&acct.projects).clone();
        let (account_id, account) = (acct.id.clone(), acct.display.clone());

        let on = |p: &Option<Named>| {
            p.as_ref()
                .map(|p| format!(" on {}", p.display))
                .unwrap_or_default()
        };
        let with = |t: &Option<Named>| {
            t.as_ref()
                .map(|t| format!(" with the {}", t.display))
                .unwrap_or_default()
        };
        let mut ents = vec![account_id];
        let add = |ents: &mut Vec<EntityId>, n: &Option<Named>| {
            if let Some(n) = n {
                ents.push(n.id.clone());
            }
        };
        let text = match kind {
            "status" => {
                add(&mut ents, &Some(project.clone()));
                add(&mut ents, &product);
                format!(
                    "Status for {account}: the {} {topic} work is {}{}.",
                    project.display,
                    rng.pick(&STATES),
                    on(&product)
                )
            }
            "issue" => {
                let ticket = self.ticket(a, rng);
                let reporter = rng.chance(1, 2).then(|| person.clone());
                ents.push(ticket.clone());
                add(&mut ents, &product);
                add(&mut ents, &reporter);
                format!(
                    "{} opened for {account}: {topic} problem{}{}.",
                    ticket.as_str().trim_start_matches("ticket:"),
                    on(&product),
                    reporter
                        .map(|p| format!(", reported by {}", p.display))
                        .unwrap_or_default()
                )
            }
            "decision" => {
                add(&mut ents, &Some(person.clone()));
                let proj = rng.chance(1, 2).then(|| project.clone());
                add(&mut ents, &proj);
                add(&mut ents, &team);
                format!(
                    "{} at {account} decided to {} the {topic} plan{}{}.",
                    person.display,
                    rng.pick(&VERBS),
                    proj.map(|p| format!(" for {}", p.display))
                        .unwrap_or_default(),
                    with(&team)
                )
            }
            "preference" => {
                add(&mut ents, &Some(person.clone()));
                add(&mut ents, &product);
                format!(
                    "{} at {account} prefers {} about {topic}{}.",
                    person.display,
                    rng.pick(&PREFS),
                    on(&product)
                )
            }
            "contact" => {
                add(&mut ents, &Some(person.clone()));
                add(&mut ents, &team);
                format!(
                    "{} is the {topic} contact at {account}{}.",
                    person.display,
                    with(&team)
                )
            }
            _ => {
                // "plan"
                add(&mut ents, &Some(project.clone()));
                add(&mut ents, &team);
                let prod = if team.is_some() { None } else { product };
                add(&mut ents, &prod);
                format!(
                    "Plan for {account}: the {} {topic} milestone moves to {}{}{}.",
                    project.display,
                    rng.pick(&MONTHS),
                    with(&team),
                    on(&prod)
                )
            }
        };
        (text, ents, kind)
    }

    /// An existing recent ticket of account `a` (60%) or a new one.
    fn ticket(&mut self, a: usize, rng: &mut SplitMix64) -> EntityId {
        let acct = &mut self.accounts[a];
        if !acct.tickets.is_empty() && rng.chance(3, 5) {
            let recent = &acct.tickets[acct.tickets.len().saturating_sub(5)..];
            return rng.pick(recent).clone();
        }
        let id = entity(format!(
            "ticket:{}-{}",
            acct.ticket_prefix, self.next_ticket
        ));
        self.next_ticket += 1;
        acct.tickets.push(id.clone());
        id
    }
}

/// Generates the dataset's rows in file order and hands each to `emit`. Returns the number
/// of distinct `(tenant, entity)` pairs mentioned.
pub fn generate(
    scale: Scale,
    seed: u64,
    mut emit: impl FnMut(Row) -> io::Result<()>,
) -> io::Result<u64> {
    let shape = scale.shape();
    let name = dataset_name(scale, seed);
    let mut root = SplitMix64::new(seed);
    let step = SPAN_SECS / shape.memories as i64;
    let mut entities = 0u64;
    for t in 0..shape.tenants {
        let tenant = TenantId::new(format!("{name}-{t}")).expect("valid tenant id");
        let mut trng = root.fork();
        let mut world = World::new(&shape, &mut trng.fork());
        let mut rng = trng.fork();
        let mut seen: HashSet<EntityId> = HashSet::new();
        for i in 0..shape.memories {
            let n = u64::from(t) * shape.memories + i;
            let (text, ents, kind) = world.memory(&mut rng);
            let unique = unique_word(n);
            let secs = EPOCH_START + i as i64 * step + rng.below(step as u64) as i64;
            seen.extend(ents.iter().cloned());
            emit(Row {
                n,
                tenant: tenant.clone(),
                text: format!("{text} Ref {unique}."),
                unique,
                entities: ents,
                labels: vec![kind.to_string()],
                as_of: OffsetDateTime::from_unix_timestamp(secs).expect("in range"),
            })?;
        }
        entities += seen.len() as u64;
    }
    Ok(entities)
}

/// Writes rows as JSONL while hashing them.
struct HashingWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl<W: Write> HashingWriter<W> {
    fn line(&mut self, row: &Row) -> io::Result<()> {
        let mut buf = serde_json::to_vec(row).map_err(io::Error::other)?;
        buf.push(b'\n');
        self.hasher.update(&buf);
        self.bytes += buf.len() as u64;
        self.inner.write_all(&buf)
    }
}

/// Generates the dataset as JSONL into `out` and returns its manifest.
pub fn write_rows(scale: Scale, seed: u64, out: impl Write) -> io::Result<Manifest> {
    let shape = scale.shape();
    let name = dataset_name(scale, seed);
    let mut w = HashingWriter {
        inner: out,
        hasher: blake3::Hasher::new(),
        bytes: 0,
    };
    let entities = generate(scale, seed, |row| w.line(&row))?;
    w.inner.flush()?;
    Ok(Manifest {
        format: FORMAT_VERSION,
        generator: GENERATOR_VERSION,
        tenants: (0..shape.tenants)
            .map(|t| TenantId::new(format!("{name}-{t}")).expect("valid tenant id"))
            .collect(),
        name,
        scale,
        seed,
        memories_per_tenant: shape.memories,
        memories: shape.memories * u64::from(shape.tenants),
        entities,
        bytes: w.bytes,
        blake3: w.hasher.finalize().to_hex().to_string(),
    })
}

/// Generates the dataset into `root/<name>/` (rows first, through a temporary file, then
/// the manifest) and returns the directory and manifest.
///
/// If the directory already has a manifest (committed manifests do, G28), the new output
/// must match it: a different hash means this build or platform generates other bytes than
/// the ones the manifest recorded, and nothing is overwritten.
pub fn write_dataset(root: &Path, scale: Scale, seed: u64) -> io::Result<(PathBuf, Manifest)> {
    let dir = root.join(dataset_name(scale, seed));
    std::fs::create_dir_all(&dir)?;
    let tmp = dir.join(format!("{ROWS}.tmp"));
    let manifest = write_rows(scale, seed, BufWriter::new(std::fs::File::create(&tmp)?))?;
    let mpath = dir.join(MANIFEST);
    if let Ok(text) = std::fs::read_to_string(&mpath) {
        let existing: Manifest = serde_json::from_str(&text).map_err(io::Error::other)?;
        if existing != manifest {
            let _ = std::fs::remove_file(&tmp);
            return Err(io::Error::other(format!(
                "generated {} with blake3 {} ({} bytes), but {} records blake3 {} ({} bytes): \
                 this build generates different data. If the generator changed on purpose, \
                 bump GENERATOR_VERSION (which gives the dataset a new name); otherwise this \
                 is a platform or determinism bug",
                manifest.name,
                manifest.blake3,
                manifest.bytes,
                mpath.display(),
                existing.blake3,
                existing.bytes
            )));
        }
    }
    std::fs::rename(&tmp, dir.join(ROWS))?;
    let mut json = serde_json::to_string_pretty(&manifest).map_err(io::Error::other)?;
    json.push('\n');
    std::fs::write(dir.join(MANIFEST), json)?;
    Ok((dir, manifest))
}
