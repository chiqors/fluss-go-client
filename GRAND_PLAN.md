# Fluss Go Client Grand Plan

This file is the long-lived implementation plan for `github.com/chiqors/fluss-go-client`.

Its purpose is to keep one shared memory of:

- what “production ready” means for this SDK
- what already exists
- what is still missing compared with the upstream Fluss Java client at `/Users/administrator/Documents/Labs/fluss/fluss-client`
- what we are actively working on next

Update this file whenever scope changes, major work lands, or we discover a new blocker.

## Checklist Status Legend

Use checklist markers consistently throughout this file:

- [ ] not started
- [~] work in progress / actively being developed
- [x] completed

Use `[~]` when work has meaningfully started but is not done yet. Once the outcome is stable and complete, convert it to `[x]`.

## Target

Build a pure Go, production-ready Fluss client SDK for Fluss `0.9.x` that feels idiomatic to Go users while pursuing feature parity with the upstream Java client.

Success means a Go team can:

- connect directly to a Fluss cluster
- manage databases and tables
- write and read log and primary-key tables
- run reliable scans and lookups
- handle retries, failures, metadata refreshes, and shutdown cleanly
- integrate into real services without requiring Java or Rust in the runtime path

## Scope

### In scope for the production target

- native Go transport and protocol handling
- stable admin API
- stable table write/read API
- metadata cache and routing
- record batch encode/decode
- Arrow-friendly read/write path
- typed errors and retry classification
- observability hooks
- authentication and security-token extension points
- integration and E2E test coverage
- examples, docs, versioning, and release process

### Out of scope for the first production-grade release

- full parity with every Java-only convenience API on day one
- reflection-heavy ORM-style struct mapper as a required core primitive
- non-`0.9.x` compatibility promises

## Current Status

### Progress Ledger

- 2026-05-30: implemented the first missing admin-mutation parity slice using the upstream Java RPC contract as the wire reference, adding public Go support for `AlterTable`, `CreatePartition`, `DropPartition`, and filtered `ListPartitionInfos`, plus regenerated proto coverage and mock integration assertions for the request shapes.
- 2026-05-30: completed the first primary-key snapshot batch-scan implementation slice by adding public snapshot storage config, a MinIO-backed remote snapshot downloader, a public `TableClient.SnapshotScanRows(...)` helper, and mock/integration coverage; after real-cluster validation showed Fluss snapshot local-reader portability is still messy across Pebble/RocksDB approaches, snapshot batch scan was pulled back out of the canonical demo support contract and remains deferred pending a cleaner implementation strategy.
- 2026-05-30: started the primary-key snapshot batch-scan vertical slice by adding upstream-aligned KV snapshot admin metadata support in the Go proto/client layer (`GetLatestKvSnapshots`, `GetKvSnapshotMetadata`, `GetLakeSnapshot`) with local integration coverage; the remote snapshot-file scanner still remains to be implemented before snapshot batch scan can be claimed in the support matrix.
- 2026-05-30: corrected compacted row/key wire semantics to follow the upstream Java client more closely for primary-key flows, including Fluss-style compacted signed varints plus compacted length-prefixed string/bytes decoding and targeted compacted PK row tests.
- 2026-05-30: taught the Go Arrow append path to honor Fluss table Arrow compression properties, including the default `ZSTD` setting, so the dedicated projection demo table can move back onto the normal default-compression path.
- 2026-05-30: added the first Fluss-specific Arrow log batch path in Go, including public Arrow append/decode helpers, mock integration coverage, a dedicated `ARROW` demo table, and real-cluster E2E intent for projection-backed log fetch semantics.
- 2026-05-30: added projection-aware public log fetch options and request-level test coverage; discovered via real-cluster validation that Fluss only supports column projection for `ARROW` log format, so the `INDEXED`-table demo and support matrix were narrowed back to partial support.
- 2026-05-30: promoted primary-key limit scan into a decoded public data operation with shared limit-scan row helpers and real-cluster E2E coverage, moving another support-matrix data-operation row out of raw-byte-only usage.
- 2026-05-30: added an explicit public indexed partial-update helper for primary-key tables and extended the Fluss+Paimon support-contract E2E to verify Java-aligned partial-update semantics through a lookup round-trip.
- 2026-05-30: started moving the canonical primary-key demo and helpers onto Fluss-documented `COMPACTED` KV format semantics, with the public Go row helpers now selecting KV row encoding from table metadata instead of assuming indexed KV rows.
- 2026-05-30: promoted primary-key typed row helpers into the public `client/` surface with decoded lookup, single-row lookup, and row upsert coverage; during follow-up validation we confirmed true snapshot batch scanning still needs the separate Java-aligned snapshot-metadata/file path before it can be claimed in the support contract.
- 2026-05-30: exposed Go-native public row/schema/type constructors and log-batch decode helpers through `client/`; validated all implemented scalar and composite data types in the Fluss+Paimon E2E all-types log round-trip.
- 2026-05-30: validated the Go demo against the real Fluss/Paimon stack, including prefix lookup round-trip on `e2e_customer_orders`; updated the demo docs and support matrix to match the current behavior.
- 2026-05-30: reworked the Fluss/Paimon Go E2E into a strict support-contract harness aligned to upstream Java client semantics for append, log limit scan, lookup, delete, and prefix lookup; updated docs and support matrix to reflect `GetTableSchema`, delete, and Paimon-backed demo coverage.

### Versioning policy

- [x] Public Go module versioning should start at `v0.1.0`

### Implemented now

- [x] Go module and package layout foundation
- [x] Native TCP RPC framing
- [x] Request multiplexing and correlation handling
- [x] Generated protobuf transport migration is complete; internal protocol handling is generated-protobuf based
- [x] API version negotiation
- [x] Basic pluggable auth interface
- [x] Metadata cache and leader routing foundation
- [x] Admin metadata calls for databases, tables, schema, and partitions
- [x] Raw table operations for append, upsert, delete, lookup, prefix lookup, fetch log, limit scan, KV scan, and demo-first KV snapshot scan
- [x] Demo E2E now exercises append, delete, limit scan, KV lookup, and prefix lookup round-trips against a real cluster
- [x] Unit and mock-style integration coverage for the current client surface
- [x] Containerized Fluss/Paimon smoke-test demo at [demo/fluss-paimon/README.md](./demo/fluss-paimon/README.md)

### Proven working now

- [x] `go test ./...`
- [x] `go build ./...`
- [x] Docker Compose E2E smoke test that boots Fluss + Flink/Paimon and validates direct Go client access
- [x] Docker Compose E2E smoke test now validates Java-aligned append, delete, limit scan, lookup, and prefix lookup behavior against the real cluster

### Not production-ready yet

- [~] Arrow record batch decoding/encoding as a first-class public API
- [~] Stable typed row abstraction beyond raw bytes
- [ ] Higher-level writer types with batching, flush, retry, and lifecycle semantics
- [ ] Higher-level scanner types with iterator or poll abstractions
- [ ] Robust reconnect and retry policy coverage under failure
- [ ] Security token / auth workflows beyond the basic no-auth path
- [ ] Metrics, tracing, and logging hooks
- [ ] Compatibility and regression matrix across real Fluss cluster scenarios
- [ ] Release engineering, examples, and public API hardening

## Production Readiness Definition

The SDK is “production ready” only when all of these are true:

- [ ] public API is intentionally versioned and documented
- [ ] the main read/write/admin flows are covered by real-cluster tests
- [ ] common failure modes have deterministic retry or surfacing behavior
- [ ] resource lifecycle is explicit and leak-resistant
- [ ] authentication/security extension points are usable
- [ ] observability hooks exist for debugging live systems
- [ ] examples compile and reflect supported patterns
- [ ] CI can catch protocol, compatibility, and regression failures

## Upstream Parity Map

This repo is explicitly tracking Java-client feature parity as the long-term product direction, while keeping the public API Go-native.

The upstream Java client includes these major areas:

- connection and metadata management
- admin APIs
- table APIs
- write subsystem
- lookup subsystem
- log scanner subsystem
- batch scanner subsystem
- converter / typed mapping helpers
- metrics
- security token management
- lake snapshot / lease workflows

This Go client should pursue parity in layers rather than trying to clone every class one-to-one.

### Parity strategy

- [x] Foundation: protocol, transport, metadata, basic admin
- [~] Raw table data operations: broad public coverage now exists for admin, log, lookup, delete, projection, KV limit-scan, and demo-first KV snapshot-scan flows, but higher-level writer/scanner ergonomics and proven real-cluster snapshot parity are still pending
- [ ] Writer subsystem parity
- [ ] Scanner subsystem parity
- [x] Lookup ergonomics parity
- [~] Typed mapping parity
- [ ] Metrics parity
- [ ] Security token parity
- [ ] Lake snapshot / lease parity

## Phase Plan

## Phase 0: Project Guardrails

Goal: make the repo easy to evolve safely.

- [ ] Define support policy for Fluss versions
- [ ] Define Go version support policy
- [x] Add CI workflow for build, unit tests, lint, and demo smoke checks
- [x] Choose linting/formatting stack
- [x] Add contribution guide
- [x] Add architecture overview doc
- [ ] Add changelog / release notes workflow

Exit criteria:

- [ ] contributors can understand repo structure and development workflow from docs alone
- [ ] CI runs on every PR

## Phase 1: Protocol and Transport Hardening

Goal: make the wire layer stable and debuggable.

### Internal protocol model direction

- [x] Public SDK surface should remain Go-native
- [x] Internal wire/protocol layer should target generated protobuf Go code
- [x] Removed dynamic proto runtime compatibility layer after generated protobuf migration landed
- [x] Introduce protobuf code generation workflow
- [x] Migrate core request builders from dynamic messages to generated protobuf structs
- [x] Keep generated protobuf code internal and avoid exposing it as the main SDK surface
- [x] Treat the proto file as the source of truth for internal wire shapes and protocol-adjacent interfaces
- [x] Prefer generated types for SDK internals and keep handwritten wrappers thin and Go-idiomatic
- [~] Audit the remaining hand-written protocol constants and helper models for proto-backed replacement where the source contract exists
- [x] Move implementation packages under `internal/` so `client/` stays the only public SDK package

### Core

- [x] Length-prefixed frame codec
- [x] API version negotiation
- [x] Correlation ID handling
- [x] Request dispatcher
- [x] Basic auth hook
- [ ] Handshake/auth state machine documentation
- [ ] Reconnect backoff policy
- [ ] Idle connection policy
- [ ] Connection pool instrumentation
- [ ] Protocol compatibility table by API key/version

### Error handling

- [x] Base API error type
- [ ] Full server error mapping audit
- [ ] `errors.Is` / `errors.As` coverage for all important classes
- [ ] Retryability classification table
- [ ] Fatal vs transient vs stale-metadata distinctions

### Tests

- [x] Frame codec unit tests
- [x] Mock integration tests for protocol flows
- [ ] Golden protocol fixtures
- [ ] Fuzzing for frame decode and message parsing
- [ ] Soak test for multiplexed concurrent requests

Exit criteria:

- [ ] transport survives reconnect, timeout, and concurrent-load scenarios in automated tests

## Phase 2: Metadata and Routing

Goal: make routing correctness boring and dependable.

### Metadata features

- [x] Bootstrap from seed endpoint
- [x] Metadata cache foundation
- [x] Basic leader routing
- [ ] Explicit cache invalidation strategy docs
- [ ] TTL and refresh tuning
- [ ] Stale leader auto-refresh rules
- [ ] Coordinator failover behavior
- [ ] Multi-endpoint bootstrap rotation
- [ ] Partitioned-table routing completeness audit

### Tests

- [x] Metadata cache tests
- [ ] Leader movement recovery tests on real cluster
- [ ] Coordinator restart recovery tests
- [ ] Tablet restart recovery tests
- [ ] Parallel metadata refresh race tests

Exit criteria:

- [ ] routing self-heals after leader or metadata changes without manual intervention

## Phase 3: Admin API Completion

Goal: ship a solid, documented admin surface.

### Essential admin

- [x] Database exists/get/list/create/drop
- [x] Table exists/get/list/create/drop
- [x] Table alter
- [x] Schema retrieval
- [x] Partition listing/info
- [x] Partition create/drop
- [ ] API documentation for each admin call
- [ ] Option structs for advanced requests where needed

### Nice-to-have parity after essentials

- [ ] Offsets/list offsets flows
- [ ] Producer offsets flows
- [ ] Cluster/rack/server-tag flows
- [ ] Rebalance flows
- [ ] ACL flows

### Tests

- [ ] Real-cluster admin lifecycle matrix
- [ ] Duplicate create / not-found / invalid-request error assertions
- [ ] Concurrent admin call coverage

Exit criteria:

- [ ] admin users can manage common resources confidently from Go

## Phase 4: Write Path

Goal: move from raw RPC operations to usable production writers.

### Foundation

- [x] Raw append RPC
- [x] Raw upsert RPC
- [ ] Raw delete convenience API audit
- [ ] Schema-aware record-batch builders
- [x] Log write batch encoder
- [x] KV write batch encoder

### Writer APIs

- [~] `AppendWriter`
- [~] `UpsertWriter`
- [ ] explicit `Flush()` semantics
- [~] explicit `Close()` semantics
- [ ] backpressure behavior
- [ ] batch size controls
- [ ] linger/flush interval controls
- [ ] per-bucket routing and retry behavior

### Partitioning / bucket assignment

- [ ] Static bucket assignment
- [ ] Hash bucket assignment
- [ ] Round-robin or sticky behavior where applicable
- [ ] Dynamic partition support audit

### Reliability

- [ ] stale metadata retry on write
- [ ] partial failure handling
- [ ] idempotence strategy decision
- [ ] timeout and cancellation semantics

### Tests

- [ ] append E2E
- [ ] upsert E2E
- [ ] delete E2E
- [ ] retry-after-leader-move E2E
- [ ] concurrent writers E2E
- [ ] cancellation/close behavior tests

Exit criteria:

- [ ] a Go service can write to Fluss using stable, documented writer types rather than raw byte RPCs

## Phase 5: Read Path and Lookup

Goal: make primary-key lookups and direct reads ergonomic.

### Current raw support

- [x] Key lookup RPC
- [x] Prefix lookup RPC
- [x] Log fetch RPC
- [x] Limit scan RPC
- [x] KV scan RPC

### Productization

- [ ] high-level lookup client
- [ ] request batching for lookups
- [ ] high-level prefix lookup API
- [ ] iterator or poll abstraction for fetch/scan
- [ ] clean scanner lifecycle management
- [ ] pagination/session handling docs

### Tests

- [ ] lookup E2E
- [x] prefix lookup E2E
- [ ] limit scan E2E with asserted records
- [ ] KV scan pagination/session E2E
- [ ] scanner close/cancel tests

Exit criteria:

- [ ] callers can read and lookup records without understanding low-level bucket RPC mechanics

## Phase 6: Scanner Subsystems

Goal: close the gap with the Java client’s richer scanner model.

### Log scanner

- [ ] log scanner abstraction
- [ ] offset initializer strategy
- [ ] continuous polling API
- [ ] fetch buffering
- [ ] remote log awareness
- [ ] watermark/high-watermark surfacing

### Batch scanner

- [ ] limit batch scanner
- [ ] KV batch scanner
- [ ] composite batch scanner
- [ ] scanner split strategy
- [ ] result ordering guarantees documentation

### Lake-aware / remote reads

- [ ] evaluate need for Paimon/lake snapshot reads in Go v1
- [ ] document which scanner modes require remote file support
- [ ] decide whether remote-file reads are in v1 or deferred

### Tests

- [ ] log scanner real-cluster tests
- [ ] remote log scanner tests
- [ ] batch scanner real-cluster tests
- [ ] mixed snapshot+log scan tests if supported

Exit criteria:

- [ ] scanner APIs cover the main Java-client reading workflows with clear Go-native semantics

## Phase 7: Arrow and Row Model

Goal: expose data in a form people can actually use efficiently.

### Data model

- [ ] define public dynamic row representation
- [ ] preserve schema ID, timestamps, offsets, and change metadata
- [ ] decide nullability and type-conversion rules

### Arrow

- [ ] choose Go Arrow dependency and version strategy
- [ ] Arrow record batch decode
- [ ] Arrow record batch encode
- [ ] Arrow-first read API
- [ ] Arrow-friendly write API

### Typed helpers

- [ ] minimal row decoder utilities
- [ ] optional mapper layer design
- [ ] explicit decision on reflection-based helpers

### Tests

- [ ] schema type coverage tests
- [ ] Arrow encode/decode round-trip tests
- [ ] compatibility tests against Fluss-produced batches

Exit criteria:

- [ ] users can consume data either as dynamic rows or Arrow batches without parsing raw bytes themselves

## Phase 8: Security and Authentication

Goal: support real deployments, not just local clusters.

### Auth

- [x] auth interface hook
- [ ] no-auth path docs
- [ ] SASL or other upstream-supported auth path investigation
- [ ] auth negotiation extensibility tests

### Tokens / remote access

- [ ] security token manager design for Go
- [ ] provider interface
- [ ] receiver registry equivalent if needed
- [ ] remote filesystem credential lifecycle

### Tests

- [ ] auth hook integration tests
- [ ] token refresh tests
- [ ] invalid-credential behavior tests

Exit criteria:

- [ ] authentication and remote data access can be integrated into secured environments cleanly

## Phase 9: Observability and Operability

Goal: make live debugging and support practical.

- [ ] logger hooks
- [ ] metrics hooks
- [ ] tracing hooks or context propagation guidance
- [ ] request latency instrumentation
- [ ] connection pool stats
- [ ] scanner/writer metric surfaces
- [ ] debug doc for common failure cases

Exit criteria:

- [ ] operators can answer “what is the client doing?” without patching the SDK

## Phase 10: Documentation, Examples, and Release

Goal: make the SDK trustworthy to adopt.

### Docs

- [ ] root README refresh to match final public API
- [ ] getting-started guide
- [ ] admin example
- [ ] append example
- [ ] upsert/lookup example
- [ ] scan example
- [ ] Docker local-dev guide
- [ ] compatibility statement

### Release

- [x] semantic versioning policy
- [ ] release checklist
- [ ] tagged example compatibility check
- [ ] CI release pipeline

Exit criteria:

- [ ] a new Go team can install, run, and understand the SDK from docs and examples alone

## Test Matrix

## Unit tests

- [x] frame encode/decode
- [x] metadata cache basics
- [x] request/response mapping
- [ ] retry classification
- [ ] row decoding
- [ ] writer batching behavior
- [ ] scanner buffering behavior
- [ ] auth/token behavior

## Integration tests

- [x] mock server request/response integration
- [ ] direct Fluss cluster integration suite
- [ ] coordinator failover scenarios
- [ ] tablet failover scenarios
- [ ] stale metadata recovery
- [ ] context cancellation behavior
- [ ] reconnect behavior
- [ ] concurrent client load

## E2E tests

- [x] Docker Compose smoke test for direct client connectivity and schema access
- [ ] append then fetch/scan E2E
- [ ] upsert then lookup E2E
- [x] prefix lookup E2E
- [ ] KV scan lifecycle E2E
- [ ] lake-enabled read-path E2E if supported in v1
- [ ] support-matrix-driven parity audit against the upstream Java client

## Current Known Gaps

- [ ] public writer/scanner abstractions are still missing
- [ ] current table data APIs are mostly raw byte oriented
- [ ] no first-class Arrow package integration yet
- [ ] no secured-cluster auth implementation yet
- [ ] no metrics/tracing integration yet
- [ ] no public examples beyond minimal snippets and demo stack
- [ ] no documented compatibility matrix beyond Fluss `0.9.x` intent

## Next Recommended Work Order

If continuing from today, the recommended order is:

1. Finish Phase 0 CI/docs guardrails.
2. Build production writer abstractions in Phase 4.
3. Productize lookup and scanner APIs in Phases 5 and 6.
4. Add row/Arrow APIs in Phase 7.
5. Add observability and security gaps in Phases 8 and 9.
6. Finish release/docs hardening in Phase 10.

## Progress Ledger

Use this section as the short memory of real repo progress.

### 2026-05-29

- [x] Established pure-Go Fluss client foundation
- [x] Added protocol, metadata, admin, and raw table RPC support
- [x] Added unit and mock integration tests
- [x] Built `demo/fluss-paimon` as a minimal real-container E2E smoke test
- [x] Removed unstable scan assertion from the demo smoke path
- [x] Simplified SQL bootstrap to schema creation only so the smoke path does not hang on streaming INSERT jobs
- [x] Added initial Go row/data helpers and wired `go-e2e` to seed sample append/upsert rows through the client API
- [x] Aligned demo prefix lookup coverage with the upstream Java client contract for bucket-key-prefix lookups
- [x] Aligned module path to `github.com/chiqors/fluss-go-client`
- [x] Updated demo README to reflect the current direct-Go validation setup
- [x] Added Phase 0 repo guardrails: CI workflow, contributing guide, and architecture overview
- [x] Added GitHub issue and PR templates and expanded root usage documentation
- [x] Recorded public versioning policy with `v0.1.0` as the first release target
- [x] Added first lightweight writer abstractions for append and upsert flows
- [x] Recorded architecture direction: Go-native public API with generated protobuf internals as the target
- [x] Added initial protobuf generation scaffold under `internal/proto/gen`
- [x] Replaced deprecated runtime `.proto` parsing with generated descriptors and message factories
- [x] Migrated admin, write, lookup, fetch, limit-scan, and KV-scan request builders to generated protobuf types
- [x] Migrated hot-path table response parsing from reflection-heavy dynamic messages to generated protobuf responses
- [x] Removed `InvokeDynamic`, deleted dynamic proto registry usage, and migrated mock integration tests to generated protobuf fixtures
- [x] Documented proto-first refactor rule for SDK internals and protocol-adjacent interfaces
- [x] Recorded follow-up to audit remaining hand-written protocol constants and local wire helpers
- [x] Moved auth, metadata, protocol, transport, and codec implementation packages under `internal/`

### 2026-05-30

- [x] Reworked `demo/fluss-paimon/go-e2e` into a support-matrix-driven harness with named checks
- [x] Tightened `CLIENT_SUPPORT_MATRIX.md` to distinguish implemented and partial Go surfaces more explicitly
- [x] Clarified that Java-client feature parity is the guiding product direction for this SDK

## Decision Log

### Active decisions already made

- [x] Compatibility target is Fluss `0.9.x` first
- [x] This is a standalone Go SDK, not a port inside the Java Maven modules
- [x] Go-native API shape is preferred over mirroring Java builders verbatim
- [x] Raw byte operations are acceptable as an early foundation
- [x] Arrow should be a first-class data path before adding rich typed mappers
- [x] Public tagged Go module versioning should begin at `v0.1.0`
- [x] Public SDK APIs should stay Go-native rather than exposing raw protobuf request/response types
- [x] Internal protocol handling should move toward generated protobuf Go code
- [x] Dynamic proto loading is a temporary implementation strategy, not the desired final architecture

### Decisions still to make

- [ ] exact public writer/scanner API shapes
- [ ] whether remote lake snapshot reads are in v1 or v1.1
- [ ] Arrow package/version policy
- [ ] first secured auth mechanism to implement
- [ ] whether typed mapping lives in root package or optional subpackage

## How To Update This File

When work lands:

- move checklist items between `[ ]`, `[~]`, and `[x]` as progress changes
- add a short note in `Progress Ledger`
- add new blockers or scope changes to `Current Known Gaps`
- update `Next Recommended Work Order` if priorities change

When a phase is effectively done:

- check every exit criterion
- add a dated note in `Progress Ledger`
- move the next phase to active focus
