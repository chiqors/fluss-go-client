# Fluss + Paimon Go E2E Demo

This demo is a trimmed-down end-to-end stack for the Go client.

It uses:

- real Fluss services
- real Flink/Paimon tiering
- a shortened `kv.snapshot.interval` in the demo Fluss services so snapshot batch scan can be exercised inside the E2E time budget
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
- primary-key table: `e2e_customers` (`COMPACTED` KV format)
- prefix-lookup table: `e2e_customer_orders` (`COMPACTED` KV format)
- all-types log table: `e2e_all_types`
- admin partition fixture table: `e2e_admin_partitions`

This table now uses the default Fluss `ARROW` compression settings, so the support-contract E2E covers Arrow projection semantics under the normal `ZSTD`-backed path rather than a special uncompressed override.

The bootstrap intentionally stops after schema creation. The Go service seeds and verifies data
itself so the Apache Fluss Go SDK proves the full round-trip through its own public client surface.

The Go service then runs a matrix-style feature harness:

- connects to Fluss using the Go SDK
- lists databases and tables
- validates database existence and database-info checks
- exercises temporary database create/drop lifecycle through the Go admin API
- validates table existence plus table metadata/schema retrieval for the bootstrap tables
- exercises temporary table create/alter/drop lifecycle through the Go admin API
- exercises partition create/list/filter/drop lifecycle against a dedicated bootstrap partitioned table
- appends indexed log rows and verifies log `LimitScan` returns the latest rows, following the upstream Java client contract
- appends Arrow log rows, fetches them back, and verifies column projection through `FetchLogWithOptions(...)` against a real `ARROW` log table
- appends and scans a dedicated all-types log row covering scalar, temporal, decimal, and nested `ARRAY/MAP/ROW` codec support
- upserts primary-key rows through the Go SDK’s table-format-aware KV helper and verifies lookup round-trips
- applies a primary-key partial update and verifies the untouched column is preserved by a follow-up lookup
- performs a primary-key limit scan after the partial update and verifies the returned rows reflect the updated compacted-table state
- waits for a real KV snapshot and performs a primary-key snapshot batch scan against the RustFS-backed remote snapshot files
- deletes a primary-key row and verifies lookup returns no value
- performs a prefix lookup against the prefix-key table and verifies the returned rows by membership rather than unsafe ordering assumptions
- fails the container if any of those paths break against the real Fluss cluster

## Verified operations

The current canonical Fluss+Paimon run is green for these real-cluster operations:

- admin: `ListDatabases`, `DatabaseExists`, `GetDatabaseInfo`, temporary `CreateDatabase`/`DropDatabase`, `ListTables`, `TableExists`, `GetTableInfo`, `GetTableSchema`, temporary `CreateTable`/`AlterTable`/`DropTable`, `CreatePartition`, `ListPartitionInfos`, `ListPartitionInfosWithSpec`, `DropPartition`
- log data: indexed-log append + limit scan, Arrow-log append + fetch + projection
- type coverage: all-types log round-trip across the currently implemented scalar and composite Go row codecs
- primary-key data: upsert, lookup, partial update, limit scan, snapshot batch scan, delete, and prefix lookup

For `ListDatabases`, some Fluss server responses currently populate `database_summary` while leaving `database_name` empty. The Go E2E log therefore prints both counts plus a resolved name list so the output reflects the actual cluster state clearly.

The current harness is the canonical support-contract E2E for the implemented Go rows in [CLIENT_SUPPORT_MATRIX.md](../../CLIENT_SUPPORT_MATRIX.md).
For overlapping features, the behavioral reference is the upstream Java client at `/Users/administrator/Documents/Labs/fluss/fluss-client`, adapted to the Go-native public API.

Cluster-global admin APIs such as cluster config mutation, server tags, rebalance control, and ACL mutation are implemented in the Go SDK and covered by mock integration tests, but they are not currently part of the canonical Fluss+Paimon demo contract because this single-tablet demo stack is not the right environment to make those global mutations a stable end-to-end proof.

The primary-key coverage is now exercised on Fluss `COMPACTED` KV tables, so the demo proves the Go SDK against the upstream Java-aligned compacted row/key semantics rather than only against the older indexed-row assumptions.

The canonical demo now includes the current primary-key snapshot batch-scan path using RustFS-backed snapshot downloads plus the Go client’s local read-only snapshot iteration. The compose stack explicitly shortens `kv.snapshot.interval` to make those snapshots appear within the one-shot E2E run. That proves the implemented path against the real demo cluster, including schema-id-aware remapping for older snapshot rows. The built-in fetcher now covers both `s3://` and local `file://`/plain-path sources; broader portability across additional filesystem backends and cluster layouts should still be treated as active follow-up work rather than fully closed risk.

The demo keeps the snapshot fetch path explicitly on S3-compatible storage by wiring the RustFS connection directly in the `go-e2e` program, so the canonical run continues to validate the remote-object-store path rather than silently falling back to local files. That explicit S3 setup is demo-only; the SDK itself still supports local `file://` and plain-path fetching by default when snapshot metadata points at local files.

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
