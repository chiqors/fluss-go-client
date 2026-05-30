# Fluss + Paimon Go E2E Demo

This demo is a trimmed-down end-to-end stack for the Go client.

It uses:

- real Fluss services
- real Flink/Paimon tiering
- a SQL bootstrap job that creates a simple lake-enabled table
- a containerized Go test service that connects directly to Fluss and validates SDK support-matrix rows in a named sequence

## What runs

- `rustfs`: S3-compatible object storage
- `zookeeper`: Fluss coordination
- `coordinator-server`: Fluss coordinator
- `tablet-server`: Fluss tablet server
- `jobmanager`: Flink JobManager
- `taskmanager`: Flink TaskManager
- `fluss-tiering-job`: Fluss lake tiering service submission
- `sql-bootstrap`: one-shot SQL setup for the E2E table
- `go-e2e`: one-shot Go client validation service

## Start the demo

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml up --build --abort-on-container-exit go-e2e
```

If the run succeeds, the `go-e2e` container exits with code `0`.

To keep the infrastructure running for inspection:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml up --build -d
docker compose -f demo/fluss-paimon/docker-compose.yml logs -f go-e2e
```

To clean up:

```bash
docker compose -f demo/fluss-paimon/docker-compose.yml down -v
```

## What the bootstrap creates

The SQL bootstrap creates a Fluss catalog plus focused support-contract tables:

- database: `fluss`
- indexed log table: `e2e_orders`
- Arrow log table: `e2e_orders_arrow` (projection-focused, using Fluss default Arrow compression settings)
- primary-key table: `e2e_customers`
- prefix-lookup table: `e2e_customer_orders`
- all-types log table: `e2e_all_types`

This table now uses the default Fluss `ARROW` compression settings, so the support-contract E2E covers Arrow projection semantics under the normal `ZSTD`-backed path rather than a special uncompressed override.

The bootstrap intentionally stops after schema creation. The Go service seeds and verifies data
itself so the Apache Fluss Go SDK proves the full round-trip through its own public client surface.

The Go service then runs a matrix-style feature harness:

- connects to Fluss using the Go SDK
- lists databases and tables
- validates database existence checks
- fetches table metadata and schema for the bootstrap tables
- appends indexed log rows and verifies log `LimitScan` returns the latest rows, following the upstream Java client contract
- appends Arrow log rows, fetches them back, and verifies column projection through `FetchLogWithOptions(...)` against a real `ARROW` log table
- appends and scans a dedicated all-types log row covering scalar, temporal, decimal, and nested `ARRAY/MAP/ROW` codec support
- upserts indexed primary-key rows and verifies KV lookup round-trips
- deletes a primary-key row and verifies lookup returns no value
- performs a prefix lookup against the prefix-key table and verifies the returned rows by membership rather than unsafe ordering assumptions
- fails the container if any of those paths break against the real Fluss cluster

The current harness is the canonical support-contract E2E for the implemented Go rows in [CLIENT_SUPPORT_MATRIX.md](../../CLIENT_SUPPORT_MATRIX.md).
For overlapping features, the behavioral reference is the upstream Java client at `/Users/administrator/Documents/Labs/fluss/fluss-client`, adapted to the Go-native public API.

The demo also proves that these Go SDK operations succeed against a real Fluss deployment configured with Paimon-backed lakehouse infrastructure. It does not claim extra lake-specific Go APIs beyond the operations it actually executes.

## Endpoints

- RustFS API: [http://localhost:9000](http://localhost:9000)
- RustFS Console: [http://localhost:9001](http://localhost:9001)
- Flink Web UI: [http://localhost:8083](http://localhost:8083)
- Fluss bootstrap from the host: `localhost:9123`
- Fluss bootstrap inside Docker: `coordinator-server:9123`

## Files

- Compose stack: [docker-compose.yml](./demo/fluss-paimon/docker-compose.yml)
- SQL bootstrap: [sql/bootstrap.sql](./demo/fluss-paimon/sql/bootstrap.sql)
- Go E2E container: [go-e2e.Dockerfile](./demo/fluss-paimon/go-e2e.Dockerfile)
- Go E2E program: [demo/fluss-paimon/go-e2e/main.go](./demo/fluss-paimon/go-e2e/main.go)
