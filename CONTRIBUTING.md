# Contributing to FogWS

Thank you for taking the time to improve FogWS.

FogWS is a pre-alpha Python WebSocket library with a Rust execution core. The
project is intentionally focused on explicit lifecycle, bounded resources,
protocol correctness, observability and measurable performance.

The current repository contains a package foundation, not a WebSocket API.
Discuss public API, runtime, protocol dependency and extension-point changes
before implementing them.

AI tools are allowed, but they don't replace engineering ownership. Every
submitted change must be understood, reviewed, tested and maintainable by its
human contributor.

## Project Channels

- Maintainer: [GefMar](https://github.com/GefMar)
- Questions and roadmap: [GitHub Discussions](https://github.com/AmberFog/fogws/discussions)
- Bugs and focused feature requests: [GitHub Issues](https://github.com/AmberFog/fogws/issues)
- Code contributions: [GitHub Pull Requests](https://github.com/AmberFog/fogws/pulls)

Don't post credentials, tokens, private traffic captures, customer data or
unpublished vulnerability details publicly. Follow [SECURITY.md](SECURITY.md)
for security reports.

## Good Contributions

Useful contributions generally make FogWS safer, clearer, more observable or
easier to validate without weakening its resource model:

- focused bug fixes with regression tests;
- protocol, lifecycle, cancellation and backpressure tests;
- documentation that matches current behavior;
- packaging and platform compatibility fixes;
- performance work with a reproducible measurement plan;
- carefully reviewed API proposals;
- security hardening and secret-redaction improvements.

Broad feature-parity patches aren't automatically a good fit. Client/server
roles, sync/async APIs, compression, proxies, reconnect and custom extensions
all change ownership, resource and compatibility contracts.

Legacy `websockets` interfaces, deprecated aliases and migration shims are out
of scope.

## Development Setup

FogWS requires Python `>=3.11`, uv and the Rust toolchain selected by
`rust-toolchain.toml`.

Build the extension:

```bash
uv run --frozen --extra dev --with "maturin>=1.13,<2" maturin develop --locked --skip-install
```

Run Python checks:

```bash
uv run --frozen --extra dev ruff format --check .
uv run --frozen --extra dev ruff check .
uv run --frozen --extra dev mypy
uv run --frozen --extra dev coverage run -m pytest
uv run --frozen --extra dev coverage report -m
```

Run Rust checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
```

Run the full local quality gate:

```bash
uv run --no-project --with pre-commit pre-commit run --all-files --show-diff-on-failure
```

## Engineering Expectations

- Keep Python as the public control surface and Rust as the execution/resource plane.
- Make connection, task, queue, timer and shutdown ownership explicit.
- Test success, error, timeout, cancellation and partial-failure paths.
- Keep protocol and resource limits bounded by default.
- Don't expose secret-bearing headers, URLs, close reasons or payloads through logs and telemetry.
- Don't call Python once per frame on a default telemetry path without measured justification.
- Reuse audited protocol/runtime components before implementing standards behavior.
- Keep abstractions and public symbols to the minimum needed by current behavior.
- Update docs and typing when public behavior changes.

## Performance Changes

Passing tests is necessary but doesn't prove a hot-path change is safe. Changes
to Rust I/O, PyO3 crossings, message assembly, buffering, compression,
telemetry, cancellation or runtime lifecycle should explain expected effects on
latency, throughput, memory, allocations, copies and task/thread counts.

Use representative workloads and report methodology with results. Don't make
performance claims from a single echo microbenchmark.

## Commit Messages

Use Conventional Commits:

```text
feat(client): add explicit connection limits
fix(protocol): preserve close outcome after cancellation
test(lifecycle): cover concurrent close and receive
docs(security): document Origin policy
```

Reference an issue only after verifying it belongs to this repository and the
change matches its scope.

## Pull Request Checklist

- The change has a focused scope and explains its ownership boundaries.
- Python and Rust checks pass where applicable.
- Public API changes update typing, tests and documentation.
- Lifecycle, cancellation and capacity behavior have negative-path tests.
- Security-sensitive surfaces don't leak secrets or disable safe defaults.
- Performance-sensitive changes include appropriate measurements.
- Remaining limitations are documented honestly.
