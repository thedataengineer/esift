# Contributing to esift

## Setup

```bash
git clone https://github.com/thekarteek/esift
cd esift
cargo build
```

Requires Rust 1.75+. Install via [rustup](https://rustup.rs).

## Running locally

```bash
docker compose -f docker/docker-compose.yml up -d
cargo run -- extract --source-url http://localhost:9200 --source-index test-logs --dest stdout
```

## Before submitting a PR

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all
```

## Adding a destination

1. Create `crates/esift-core/src/dest/your_dest.rs`
2. Implement the `Destination` trait from `dest/mod.rs`
3. Export it from `dest/mod.rs`
4. Wire it into the CLI match arm in `crates/esift-cli/src/main.rs`

## Adding a source

Same pattern under `crates/esift-core/src/source/`.

## Issues and PRs

Open an issue before starting significant work so we can discuss the approach first.
