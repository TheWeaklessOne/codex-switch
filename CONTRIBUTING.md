# Contributing

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
