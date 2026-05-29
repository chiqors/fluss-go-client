# Contributing

Thanks for contributing to `github.com/chiqors/fluss-client-go`.

This project is building toward a production-ready pure Go Fluss client SDK. The repo already has a working foundation, but many higher-level APIs and production hardening tasks are still in progress. Please keep contributions incremental, tested, and aligned with the project plan.

## Before You Start

Read these first:

- [AGENTS.md](/Users/administrator/Documents/Labs/fluss-client/AGENTS.md)
- [GRAND_PLAN.md](/Users/administrator/Documents/Labs/fluss-client/GRAND_PLAN.md)
- [README.md](/Users/administrator/Documents/Labs/fluss-client/README.md)

Use the upstream Java client at `/Users/administrator/Documents/Labs/fluss/fluss-client` as a behavioral reference when needed, but do not assume the Go SDK should mirror Java package or API shapes exactly.

## Development Principles

- Prefer Go-native APIs and naming.
- Keep `context.Context` on all networked operations.
- Keep public APIs intentionally small and stable.
- Favor incremental, test-backed changes over broad rewrites.
- Update docs and plan state when behavior or scope changes.

## Local Setup

Recommended local checks:

```bash
gofmt -w $(find . -name '*.go' -not -path './.git/*')
go test ./...
go build ./...
```

If you change the demo stack under [demo/fluss-paimon](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon), also run:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml config
docker compose -f demo/fluss-paimon/docker-compose.yml up --build --abort-on-container-exit go-e2e
docker compose -f demo/fluss-paimon/docker-compose.yml down -v
```

## Pull Request Expectations

Each non-trivial change should include:

- code changes scoped to a clear subsystem or plan phase
- tests or verification appropriate to the risk
- documentation updates if the public behavior changed
- `GRAND_PLAN.md` updates when checklist status, blockers, or roadmap assumptions changed

## Code Style

- Run `gofmt` on touched Go files.
- Prefer explicit error wrapping with useful context.
- Avoid exporting unstable internals too early.
- Keep long-lived resources explicit with `Close()`.
- Document concurrency expectations for any type that may be shared.

## Reporting Progress

This repo uses [GRAND_PLAN.md](/Users/administrator/Documents/Labs/fluss-client/GRAND_PLAN.md) as persistent project memory.

When your work materially advances the project:

- mark checklist items
- add a short dated note to the `Progress Ledger`
- add any newly discovered blockers or gaps

## Scope Notes

Near-term priority is Phase 0 through Phase 6 in the grand plan:

- repo guardrails and CI
- production-grade writer abstractions
- lookup and scanner productization
- stronger real-cluster and E2E validation

If you want to propose a larger design change, document the tradeoff clearly before expanding the public surface.
