# Contributing to VaultFS

Thank you for your interest in contributing to VaultFS! This guide will help you get started.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Prerequisites

- **Rust toolchain** (stable, 1.82+): Install via [rustup](https://rustup.rs/)
- **Git**
- **Docker** (optional, for container builds)

## Getting Started

### Building from Source

```bash
# Clone the repository
git clone https://github.com/aniasse/vaultfs.git
cd vaultfs

# Build in debug mode
cargo build

# Build release binary
cargo build --release

# The binary is at ./target/release/vaultfs
```

### Running Locally

```bash
# Run with default config (listens on :8000, stores data in ./data)
cargo run

# Run with a custom config
VAULTFS_CONFIG=config/default.toml cargo run

# Run with debug logging
RUST_LOG=debug cargo run
```

### Running Tests

```bash
# Run all workspace tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p vaultfs-storage
cargo test -p vaultfs-auth
cargo test -p vaultfs-processing

# Run integration tests
cargo test -p vaultfs-tests
```

## Architecture Overview

VaultFS is organized as a Cargo workspace with the following crates:

| Crate | Description |
|---|---|
| `vaultfs-server` | HTTP server built on Axum, routes, middleware, and the CLI entry point |
| `vaultfs-storage` | Storage engine with SQLite metadata and content-addressable file storage |
| `vaultfs-processing` | Image transformation pipeline (resize, crop, format conversion) |
| `vaultfs-cache` | Multi-level cache (in-memory LRU + disk) for processed images |
| `vaultfs-auth` | API key management, permission model, and presigned URL generation |
| `vaultfs-s3-compat` | S3-compatible API layer (ListBuckets, GetObject, PutObject, etc.) |
| `vaultfs-cli` | CLI argument parsing and configuration loading |
| `vaultfs-tests` | Integration and end-to-end tests |

### Request Flow

```
Client Request
    |
    v
vaultfs-server (Axum router + auth middleware)
    |
    +---> vaultfs-auth (validate API key / presigned URL)
    |
    +---> vaultfs-storage (read/write objects, SQLite metadata)
    |
    +---> vaultfs-processing (transform images on the fly)
    |
    +---> vaultfs-cache (check/store processed results)
    |
    +---> vaultfs-s3-compat (handle S3 protocol requests)
```

## Code Style

- Run `cargo fmt` before committing. The project uses the default `rustfmt` configuration.
- Run `cargo clippy --workspace` and fix all warnings.
- Follow standard Rust naming conventions and idioms.
- Keep functions focused and small. Prefer returning `Result` over unwrapping.
- Add doc comments (`///`) to all public items.

## Making Changes

### Branch Naming

- `fix/short-description` for bug fixes
- `feat/short-description` for new features
- `docs/short-description` for documentation
- `refactor/short-description` for refactoring

### Commit Messages

Write clear, concise commit messages. Use the imperative mood:

- "Add presigned URL expiration validation"
- "Fix cache eviction when disk is full"
- "Refactor storage engine to support pluggable backends"

### Pull Request Process

1. **Fork** the repository and create a branch from `main`.
2. **Make your changes** following the code style guidelines above.
3. **Add tests** for any new functionality or bug fixes.
4. **Run the full test suite** to make sure nothing is broken:
   ```bash
   cargo fmt --check
   cargo clippy --workspace
   cargo test --workspace
   ```
5. **Open a pull request** against `main`. Fill in the PR template.
6. **Address review feedback** by pushing additional commits (do not force-push during review).
7. Once approved, a maintainer will merge your PR.

### What Makes a Good PR

- Solves one focused problem.
- Includes tests that cover the change.
- Does not introduce unnecessary dependencies.
- Updates documentation and changelog if the change is user-facing.

## Reporting Bugs

Use the [Bug Report](https://github.com/aniasse/vaultfs/issues/new?template=bug_report.md) issue template. Include reproduction steps, your environment, and any relevant logs.

## Requesting Features

Use the [Feature Request](https://github.com/aniasse/vaultfs/issues/new?template=feature_request.md) issue template. Explain the use case and proposed solution.

## Security Vulnerabilities

Do **not** open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md) for responsible disclosure instructions.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
