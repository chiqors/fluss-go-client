# Fluss + Paimon Go E2E Demo

This demo is a trimmed-down end-to-end stack for the Go client.

It uses:

- real Fluss services
- real Flink/Paimon tiering
- a SQL bootstrap job that creates a simple lake-enabled table
- a containerized Go test service that connects directly to Fluss and validates multiple SDK feature surfaces

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

The SQL bootstrap creates a Fluss catalog plus three simple tables:

- database: `fluss`
- log table: `e2e_orders`
- primary-key table: `e2e_customers`
- prefix-lookup table: `e2e_customer_orders`

The bootstrap intentionally stops after schema creation. The demo is focused on proving that the
Go SDK can connect, inspect metadata, and exercise read-path entry points without depending on a
long-running Flink insert job.

The Go service then:

- connects to Fluss using the Go SDK
- lists databases and tables
- validates database and table existence checks
- fetches table metadata and schema for both tables
- seeds one indexed log row and one indexed KV row through the Go client
- validates table metadata for the bootstrap tables
- runs a real `LimitScan` against the log table
- performs a KV lookup against the primary-key table and verifies the stored row round-trip
- performs a prefix lookup against the prefix-key table and verifies the returned rows round-trip
- fails the container if any of those paths break against the real Fluss cluster

This demo currently validates admin, metadata, write, log-table scan entry, KV lookup, and prefix
lookup round-trip against a real cluster.

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
