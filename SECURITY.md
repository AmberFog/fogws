# Security Policy

FogWS is security-sensitive because it will process untrusted network input,
perform HTTP Upgrade and WebSocket framing, negotiate compression, manage TLS
and proxies, and expose protocol state through a Python/Rust boundary.

## Supported Versions

Security fixes are provided on a best-effort basis for the active development
line and latest published release when one exists.

| Version | Security support |
|---|---|
| `main` | Active fixes land here first |
| latest release | Best-effort fixes and patch releases |
| older releases | Not guaranteed |

## Reporting A Vulnerability

Don't open a public issue for a suspected vulnerability.

1. Use GitHub private vulnerability reporting when it is available.
2. Otherwise contact [GefMar](https://github.com/GefMar) through GitHub without
   posting vulnerability details publicly.
3. If coordination must start in
   [GitHub Discussions](https://github.com/AmberFog/fogws/discussions), keep the
   public message minimal.

Include the affected version or commit, Python/Rust/platform details, a minimal
reproduction, expected impact and whether the issue is already public. Redact
tokens, cookies, authorization headers, private certificates, traffic captures
and customer data.

## Security-Sensitive Areas

- TLS certificate verification and trust roots;
- HTTP Upgrade parsing, header limits and request smuggling boundaries;
- Origin validation and Cross-Site WebSocket Hijacking;
- proxy authentication and redirect credential handling;
- frame masking, opcodes, fragmentation and UTF-8 validation;
- close codes and reasons from untrusted peers;
- message, fragment, queue and connection memory limits;
- Per-Message Deflate amplification and compression state;
- Ping/Pong, timeouts, slow peers and denial of service;
- cancellation, shutdown and task/socket leaks;
- Python/Rust lifetime, GIL and free-threading behavior;
- secrets or payloads in errors, repr, logs, metrics, traces and diagnostics;
- panics, memory-safety bugs or undefined behavior triggered by network input.

## Required Security Defaults

FogWS development must preserve these principles:

- TLS verification is enabled by default.
- Network and handshake input is bounded and validated.
- Queues and message assembly have explicit limits and backpressure.
- Server Origin policy is explicit and documented.
- Telemetry excludes secrets and avoids unbounded/high-cardinality labels.
- Protocol and compression behavior is delegated to audited dependencies where practical.
- Unsafe Rust requires an explicit architecture and security review; the current crate forbids it.

## Coordinated Disclosure

The maintainer will make a best-effort attempt to acknowledge reports, confirm
affected revisions, coordinate a fix and credit reporters who want credit.
Please allow time to investigate and release a fix before publishing exploit
details.
