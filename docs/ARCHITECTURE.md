# Architecture Overview

This document explains the current architecture of `github.com/chiqors/fluss-client-go`.

It is intentionally short and should evolve with the implementation.

## Goals

The SDK is designed to:

- talk directly to Fluss over its native RPC protocol
- provide an idiomatic Go API over Fluss admin and table workflows
- keep protocol and routing details internal where possible
- grow from a raw-but-correct foundation into a production-ready client

## Package Layout

### Public-facing packages

- [client](/Users/administrator/Documents/Labs/fluss-client/client)
  - entry point for dialing the cluster
  - admin and table-level operations
  - user-facing config and result types

### Protocol and transport

- [rpc](/Users/administrator/Documents/Labs/fluss-client/rpc)
  - connection management
  - request dispatch and response correlation
  - API negotiation and auth hook integration

- [codec](/Users/administrator/Documents/Labs/fluss-client/codec)
  - Fluss frame encode/decode logic

- [protocol](/Users/administrator/Documents/Labs/fluss-client/protocol)
  - API key definitions
  - protocol-level error types

### Metadata and routing

- [metadata](/Users/administrator/Documents/Labs/fluss-client/metadata)
  - table path and routing metadata helpers
  - metadata cache used by the client layer

### Internal support

- [auth](/Users/administrator/Documents/Labs/fluss-client/auth)
  - pluggable auth interfaces

- [internal/proto](/Users/administrator/Documents/Labs/fluss-client/internal/proto)
  - embedded Fluss proto descriptors

- [internal/pbutil](/Users/administrator/Documents/Labs/fluss-client/internal/pbutil)
  - protobuf helper utilities

### Demo environment

- [demo/fluss-paimon](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon)
  - real-container smoke-test environment
  - validates direct Go client access against a local Fluss stack

## Current Request Flow

At a high level:

1. `client.Dial(...)` builds a root client using configured endpoints.
2. The RPC layer negotiates supported API versions with the cluster.
3. Metadata is fetched and cached for routing.
4. Admin or table methods construct protobuf-backed requests.
5. The RPC client selects the right server, sends the framed request, and matches the response by correlation ID.
6. Results are returned as Go types or raw record-batch bytes, depending on the API.

## Current Tradeoffs

The repo is intentionally early in a few areas:

- many data-plane APIs still return raw wire-format bytes
- higher-level writer and scanner abstractions are not complete yet
- Arrow integration is planned but not implemented
- secured-cluster support is still mostly an extension-point design, not a full implementation

These tradeoffs are deliberate. The current priority is correctness of protocol behavior and a stable foundation before expanding the high-level API surface.

## Direction Of Travel

The next major steps are:

1. repo guardrails and CI
2. production-grade writer abstractions
3. high-level lookup and scanner APIs
4. Arrow and row-model support
5. security and observability hardening

See [GRAND_PLAN.md](/Users/administrator/Documents/Labs/fluss-client/GRAND_PLAN.md) for the detailed roadmap and current checklist state.
