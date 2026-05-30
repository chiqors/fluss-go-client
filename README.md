# fluss-go-client

Unofficial Go client SDK for Apache Fluss `0.9.x`.

This repository is building a Go-native Fluss client without requiring Java in the application runtime path.

## Status

The SDK is real, working, and actively developed, but it is not production-ready yet.

Today it already includes:

- direct TCP client connectivity to Fluss
- admin APIs for database, table, schema, and partition operations
- data operations for append, upsert, partial update, delete, lookup, prefix lookup, fetch log, and limit scan
- public row/schema/type helpers for the currently implemented data types
- Arrow log fetch/projection support on Fluss `ARROW` log tables
- buffered `AppendWriter` and `UpsertWriter` with explicit `Flush(ctx)` and flush-on-close behavior
- a real Fluss + Paimon container demo used as the canonical end-to-end support check

## What Is Supported

High level:

- Fluss `0.9.x`
- Go-native admin workflows
- log-table writes and reads
- primary-key upsert, lookup, partial update, delete, prefix lookup, and limit scan
- implemented scalar and composite Fluss data types in the current row helpers
- validation against a Fluss deployment configured with Paimon-backed lakehouse infrastructure

For the detailed support matrix, see [CLIENT_SUPPORT_MATRIX.md](./CLIENT_SUPPORT_MATRIX.md).

## What Is Not Supported Yet

Important gaps still remain:

- production-grade scanner abstractions
- full snapshot batch-scan parity in the canonical real-cluster demo
- hardened retry/reconnect behavior under failure
- secured-cluster auth and token workflows beyond the basic hook surface
- metrics and tracing hooks
- full compatibility/regression coverage across more cluster scenarios
- polished examples and final public API documentation

## Why It Is Not Production Ready Yet

The main reasons are:

- some public surfaces are still low-level compared with the upstream Java client
- writer ergonomics have started, but retry and failure-handling behavior still need hardening
- scanner ergonomics are not finished yet
- real-cluster coverage is good for the current support-contract flows, but not broad enough for production confidence
- observability and secured-environment support are still incomplete

The long-lived roadmap is in [GRAND_PLAN.md](./GRAND_PLAN.md).

## Quick Usage

Install:

```bash
go get github.com/chiqors/fluss-go-client@latest
```

Connect and list databases:

```go
package main

import (
	"context"
	"log"

	"github.com/chiqors/fluss-go-client/client"
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

	names, summaries, err := cli.Admin().ListDatabases(ctx, true)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("database names: %v", names)
	log.Printf("database summaries: %v", summaries)
}
```

Get table metadata:

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

## Developer Quick Guide

Useful docs:

- [GRAND_PLAN.md](./GRAND_PLAN.md): roadmap and project memory
- [CLIENT_SUPPORT_MATRIX.md](./CLIENT_SUPPORT_MATRIX.md): current support status
- [AGENTS.md](./AGENTS.md): repo working rules
- [CONTRIBUTING.md](./CONTRIBUTING.md): contribution workflow
- [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md): current architecture
- [demo/fluss-paimon/README.md](./demo/fluss-paimon/README.md): real-cluster demo contract

Useful local checks:

```bash
gofmt -w $(find . -name '*.go' -not -path './.git/*')
go test ./...
go build ./...
```

If you touch the Fluss + Paimon demo:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml config
docker compose -f demo/fluss-paimon/docker-compose.yml up --build --abort-on-container-exit go-e2e
docker compose -f demo/fluss-paimon/docker-compose.yml down -v
```

The upstream Java client at `/Users/administrator/Documents/Labs/fluss/fluss-client` is the behavioral reference for overlapping semantics. This repo keeps the public API Go-native while using upstream Java behavior and Fluss protocol definitions as the source of truth.

## Contributing

Contributions are welcome.

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) first, and keep these expectations in mind:

- preserve Go-native public APIs
- use the upstream Java client as a behavioral reference, not as a package-structure template
- update docs and [GRAND_PLAN.md](./GRAND_PLAN.md) when meaningful progress or blockers change
- verify changes with tests proportional to risk

## License

This repository is licensed under the Apache License 2.0. See [LICENSE](./LICENSE).
