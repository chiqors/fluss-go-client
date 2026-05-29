# AGENTS.md

This file explains how agents should work in `github.com/chiqors/fluss-go-client`.

The goal is consistency: future work should preserve the SDK’s design direction, keep the repo usable, and leave behind clear progress markers.

## Mission

Build and maintain a pure Go Fluss client SDK that is:

- idiomatic for Go users
- aligned with Fluss `0.9.x`
- grounded in real protocol behavior
- safe to evolve toward production readiness

Agents working in this repo should optimize for correctness, testability, and continuity of project memory.

## Primary Sources Of Truth

When making decisions, use these in order:

1. [GRAND_PLAN.md](./GRAND_PLAN.md)
2. the current repository code
3. the upstream Java Fluss client at `/Users/administrator/Documents/Labs/fluss/fluss-client`
4. official Fluss documentation and protocol definitions

If these disagree:

- preserve correctness over convenience
- preserve explicit repo decisions already recorded in `GRAND_PLAN.md`
- document any changed assumption in `GRAND_PLAN.md`

## Repo Intent

This repo is not meant to be a literal Java port.

Agents should keep these design principles:

- prefer Go-native API shapes over Java-style builders when possible
- keep `context.Context` on all networked operations
- make lifecycle explicit with `Close()` on long-lived resources
- use standard Go `error` behavior, plus typed errors where callers need inspection
- avoid exposing low-level wire details unless they are intentionally part of the SDK surface
- avoid overcommitting unstable APIs too early
- keep protobuf and wire-contract details as an internal implementation concern rather than the public SDK surface

## Current Architecture

Important packages:

- [client](./client): public-facing client/admin/table operations
- [rpc](./rpc): transport client and request dispatch
- [codec](./codec): frame encode/decode
- [metadata](./metadata): metadata cache and routing helpers
- [protocol](./protocol): API keys and protocol-level errors
- [auth](./auth): auth hooks
- [internal/proto](./internal/proto): embedded proto descriptors
- [demo/fluss-paimon](./demo/fluss-paimon): real-container smoke-test environment

Current state:

- the foundation is real and working
- many table operations are still raw byte-oriented
- production-grade writer/scanner abstractions are not complete yet

## How To Work In This Repo

Before making substantial changes:

- read the relevant section of [GRAND_PLAN.md](./GRAND_PLAN.md)
- inspect existing code paths before proposing new structure
- check whether the upstream Java client already has a corresponding subsystem

When implementing:

- keep changes focused on one phase or one coherent subsystem where possible
- prefer incremental, test-backed progress over sweeping rewrites
- preserve backward compatibility for public APIs unless there is a strong reason not to
- if a public API must change, update docs and note it in `GRAND_PLAN.md`
- prefer Go-native public types even when internal transport uses protobuf-generated code

When blocked:

- leave behind the blocker clearly in `GRAND_PLAN.md` under progress or gaps
- do not silently abandon half-finished architectural changes

## Required Workflow For Any Non-Trivial Change

1. Identify which phase in `GRAND_PLAN.md` the work belongs to.
2. Inspect related implementation and tests first.
3. Make the smallest coherent code change that advances that phase.
4. Add or update tests.
5. Run the relevant verification commands.
6. Update `GRAND_PLAN.md` if progress, scope, or known gaps changed.
7. If docs or examples are affected, update them in the same change.

## Progress Memory Rules

Agents must treat [GRAND_PLAN.md](./GRAND_PLAN.md) as persistent project memory.

Checklist marker convention:

- `[ ]` means not started
- `[~]` means in progress / active work in development
- `[x]` means completed

Update it when:

- checklist items are completed
- a new blocker is discovered
- priorities change
- a phase boundary is crossed
- a major architectural decision is made

At minimum, meaningful work should leave behind:

- updated checklist state
- a short dated note in the `Progress Ledger`

When work is started but not finished, prefer marking the relevant item as `[~]` instead of leaving it as `[ ]`.

## Testing Expectations

Do not treat a change as complete without verification proportional to the risk.

### Minimum verification

For normal Go code changes:

- run `gofmt` on touched Go files
- run `go test ./...`
- run `go build ./...` if public APIs or package wiring changed

### For demo or environment changes

If touching [demo/fluss-paimon](./demo/fluss-paimon):

- validate compose config
- run the demo smoke test when the change can affect it

Preferred commands:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml config
docker compose -f demo/fluss-paimon/docker-compose.yml up --build --abort-on-container-exit go-e2e
docker compose -f demo/fluss-paimon/docker-compose.yml down -v
```

### For protocol or routing changes

Prefer adding:

- unit tests
- mock integration tests
- real-cluster validation when behavior could diverge under failure or metadata movement

## Coding Standards

### Go style

- keep code idiomatic and readable
- prefer explicitness over cleverness
- keep exported APIs small and stable
- keep internal helpers internal until the shape is proven
- avoid unnecessary abstractions

### Errors

- return actionable errors
- wrap lower-level errors with context
- use typed errors where callers need retry or classification behavior
- do not swallow protocol or routing failures

### Context and lifecycle

- all networked operations should take `context.Context`
- long-lived resources should have clear `Close()` behavior
- cancellation and timeouts should be honored where possible

### Concurrency

- document concurrency assumptions explicitly
- default to making `Client` safe for concurrent use
- do not assume table writers/scanners are concurrency-safe unless intentionally designed that way

## API Design Guardrails

Prefer:

- option structs over long parameter lists
- lightweight helper clients over giant god objects
- explicit constructors such as `client.Dial(...)`
- Go naming over Java naming when the concepts are equivalent
- generated protobuf Go code as an internal transport detail when the protocol layer matures

Avoid:

- Java-style builder sprawl unless it clearly improves the Go UX
- exposing unstable internals as public types too early
- locking the public API around today’s raw-byte representations if a better abstraction is imminent
- making application code depend directly on protobuf request/response structs unless there is a very strong reason

## Upstream Java Client Usage

Use the Java client as a reference for:

- protocol behavior
- routing semantics
- writer/scanner behavior
- snapshot, lookup, and token workflows
- test scenarios worth reproducing

Do not assume the Go package structure should mirror Java one-for-one.

When porting behavior:

- match semantics first
- adapt API shape second
- record any intentional deviation in `GRAND_PLAN.md`

For protocol implementation strategy:

- dynamic proto loading is acceptable as a bootstrap step
- generated protobuf Go code is the preferred long-term internal implementation
- public SDK APIs should remain Go-native even after the internal migration

## Demo Environment Guidance

The demo under [demo/fluss-paimon](./demo/fluss-paimon) is a real smoke-test environment, not just sample code.

Treat it as:

- a proof that the Go client can talk to a real Fluss cluster
- a place to add stronger E2E flows over time

Do not make the demo README misleading. Keep it aligned with what the demo actually verifies today.

## Documentation Rules

When behavior changes, update the relevant docs in the same work:

- [README.md](./README.md)
- [GRAND_PLAN.md](./GRAND_PLAN.md)
- [demo/fluss-paimon/README.md](./demo/fluss-paimon/README.md)

If a capability is partial or experimental, say so plainly.

## What Not To Do

- do not claim production readiness before the checklist supports it
- do not add large undocumented public APIs casually
- do not leave broken imports, stale docs, or mismatched module paths
- do not hide blockers that affect roadmap confidence
- do not replace real verification with guesswork when the repo can be tested locally

## Good End State For A Task

A task is in good shape when:

- code is coherent
- tests pass for the affected area
- docs are updated if behavior changed
- `GRAND_PLAN.md` reflects the new reality
- the next person can understand what was done and what remains

## Quick Start For Future Agents

When starting work here:

1. Read [GRAND_PLAN.md](./GRAND_PLAN.md).
2. Check current repo status and recent changes.
3. Inspect the relevant package and tests.
4. Use the upstream Java client only as a behavioral reference.
5. Make small, verifiable progress.
6. Leave the plan and docs better than you found them.
