# GraphCache task runner. Run `just` to list recipes.

set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# List recipes
default:
    @just --list

# fmt check, clippy with warnings as errors, all tests
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# Format the whole workspace
fmt:
    cargo fmt --all

# Run all tests
test:
    cargo test --workspace

# Licences, advisories, bans and sources (needs cargo-deny)
deny:
    cargo deny check

# Start Neo4j, MinIO, Prometheus and Grafana
up:
    @echo "owned by Person B, not set up yet"

# Load a synthetic dataset into the local stack
load:
    @echo "owned by Person B, not set up yet"

# Run the memharness benchmark
bench:
    @echo "owned by Person B, not set up yet"

# memharness small run (referenced by CLAUDE.md)
bench-small:
    @echo "owned by Person B, not set up yet"
