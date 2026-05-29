# fluss-client-go

Pure Go Fluss client SDK for Fluss `0.9.x`.

This repository is building toward a production-ready, Go-native client for Apache Fluss without requiring Java in the application runtime path.

## Status

The project already has a working core foundation, but it is not fully production-ready yet.

Current implementation status:

- Native TCP RPC framing and request multiplexing
- Runtime loading of Fluss protobuf descriptors from embedded `.proto`
- API version negotiation and pluggable auth hook
- Metadata cache and bucket leader routing
- Admin APIs for database/table/schema/partition metadata
- Raw table operations for append, upsert, lookup, prefix lookup, fetch log, limit scan, and KV scan

The current data APIs operate on Fluss wire-format record batches as raw bytes. Arrow-first row
encoders/decoders and richer typed row helpers are intentionally left as the next layer on top of
this foundation.

## Current Scope

Today, this repo is best understood as:

- a real protocol and metadata foundation
- a usable admin and low-level table client
- an active implementation project with a tracked roadmap

Still in progress:

- higher-level writer abstractions
- higher-level scanner abstractions
- Arrow-first row APIs
- stronger secured-cluster support
- broader failure-mode and E2E coverage

## Install

```bash
go get github.com/chiqors/fluss-client-go
```

Common install patterns:

- stable latest tag: `go get github.com/chiqors/fluss-client-go@latest`
- pinned release: `go get github.com/chiqors/fluss-client-go@v0.1.0`
- moving main branch: `go get github.com/chiqors/fluss-client-go@main`

## Quick Start

Create a root client with one or more Fluss bootstrap endpoints:

```go
package main

import (
	"context"
	"log"

	"github.com/chiqors/fluss-client-go/client"
)

func main() {
	ctx := context.Background()

	cli, err := client.Dial(ctx, client.Config{
		Endpoints: []string{"127.0.0.1:9123"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	admin := cli.Admin()
	databases, _, err := admin.ListDatabases(ctx, false)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("databases: %d", len(databases))
}
```

## Usage

### Connect

```go
ctx := context.Background()
cli, err := client.Dial(ctx, client.Config{
	Endpoints: []string{"127.0.0.1:9123"},
})
if err != nil {
	log.Fatal(err)
}
defer cli.Close()
```

### Admin operations

```go
admin := cli.Admin()

exists, err := admin.TableExists(ctx, client.TablePath{
	DatabaseName: "fluss",
	TableName:    "orders",
})
if err != nil {
	log.Fatal(err)
}

_ = exists
```

### Table metadata

```go
table := cli.Table(client.TablePath{
	DatabaseName: "fluss",
	TableName:    "orders",
})

info, err := table.Info(ctx)
if err != nil {
	log.Fatal(err)
}

schema, err := table.Schema(ctx, nil)
if err != nil {
	log.Fatal(err)
}

_, _ = info, schema
```

### Raw data-plane operations

The current data-plane API is still low-level. Read and write methods mostly work with Fluss wire-format record-batch bytes rather than decoded row objects.

That means this SDK is already useful for protocol work, integration work, and service foundations, but it is still growing toward more ergonomic application-facing APIs.

## Local Demo

A real containerized smoke-test environment is available under [demo/fluss-paimon](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon).

It boots:

- Fluss coordinator and tablet server
- Flink/Paimon tiering components
- SQL bootstrap for a test table
- a containerized Go E2E checker

Start it with:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml up --build --abort-on-container-exit go-e2e
```

See [demo/fluss-paimon/README.md](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon/README.md) for details.

## Development

Useful local checks:

```bash
gofmt -w $(find . -name '*.go' -not -path './.git/*')
go test ./...
go build ./...
```

Project docs:

- [GRAND_PLAN.md](/Users/administrator/Documents/Labs/fluss-client/GRAND_PLAN.md): roadmap and progress memory
- [AGENTS.md](/Users/administrator/Documents/Labs/fluss-client/AGENTS.md): repo working rules
- [CONTRIBUTING.md](/Users/administrator/Documents/Labs/fluss-client/CONTRIBUTING.md): contribution workflow
- [docs/ARCHITECTURE.md](/Users/administrator/Documents/Labs/fluss-client/docs/ARCHITECTURE.md): current architecture

## Compatibility

- Target Fluss compatibility: `0.9.x`
- Module path: `github.com/chiqors/fluss-client-go`

Compatibility beyond `0.9.x` is not promised yet.

## Versioning

This repository should publish tagged semantic versions for Go consumers.

- first public release: `v0.1.0`
- pre-`v1` releases may still refine the API surface
- breaking API changes before `v1.0.0` should still be called out clearly in release notes and docs

For `go get`, users should prefer tagged versions over floating commits once releases are published.
