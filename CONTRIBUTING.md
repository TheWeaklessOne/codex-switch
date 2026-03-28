# Contributing

Please read [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) before participating.

## Development Expectations

- keep changes scoped and production-oriented
- prefer explicit domain models over implicit JSON blobs
- preserve deterministic filesystem behavior and rollback safety
- avoid silent destructive writes to managed state
- add tests for new behavior and regressions

## Local Checks

Before opening a pull request, run:

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Compatibility Notes

- the project is validated primarily against `codex-cli 0.115.0`
- the default managed runtime root remains `~/.telex-codex-switcher` for backward compatibility
- experimental workspace forcing should stay probe-gated unless a change explicitly tightens that contract

## Public Repository Notes

- use the issue templates for bugs and feature requests
- route vulnerabilities through [SECURITY.md](./SECURITY.md), not public issues
- use [SUPPORT.md](./SUPPORT.md) for the expected reporting and support paths
