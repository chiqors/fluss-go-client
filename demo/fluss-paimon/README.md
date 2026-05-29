# Fluss + Paimon Go E2E Demo

This demo is a trimmed-down end-to-end stack for the Go client.

It removes the old ingestion path through Redpanda, Fluss CAPE, and `fhir-olap-pipes`, and replaces it with:

- real Fluss services
- real Flink/Paimon tiering
- a SQL bootstrap job that creates a simple lake-enabled table
- a containerized Go test service that connects directly to Fluss and validates the result

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

The SQL bootstrap creates a Fluss catalog and a simple lake-enabled log table:

- database: `fluss`
- table: `e2e_orders`

The Go service then:

- connects to Fluss using the Go SDK
- fetches table metadata
- validates the schema lookup path
- fails the container if the table cannot be resolved through the Go client

This demo currently validates bootstrap, metadata, and schema access. It does not depend on Flink SQL data insertion, which is intentionally left out of the smoke test because that write path was not stable in this environment.

## Endpoints

- RustFS API: [http://localhost:9000](http://localhost:9000)
- RustFS Console: [http://localhost:9001](http://localhost:9001)
- Flink Web UI: [http://localhost:8083](http://localhost:8083)
- Fluss bootstrap from the host: `localhost:9123`
- Fluss bootstrap inside Docker: `coordinator-server:9123`

## Files

- Compose stack: [docker-compose.yml](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon/docker-compose.yml)
- SQL bootstrap: [sql/bootstrap.sql](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon/sql/bootstrap.sql)
- Go E2E container: [go-e2e.Dockerfile](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon/go-e2e.Dockerfile)
- Go E2E program: [demo/fluss-paimon/go-e2e/main.go](/Users/administrator/Documents/Labs/fluss-client/demo/fluss-paimon/go-e2e/main.go)
