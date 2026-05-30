# Client Feature Support Matrix

Fluss has a rich set of features and native data types available to users. The tables below summarize what the upstream Java client supports today and what the Go client in this repo supports today.

The current real-cluster support contract is exercised by the Fluss+Paimon demo under `demo/fluss-paimon`.
That E2E harness is currently green for the implemented Go `client/` surface covering database/table/partition admin lifecycle, indexed log append + limit scan, Arrow log fetch + projection, all-types log round-trip, primary-key upsert + lookup, primary-key partial update, primary-key limit scan, primary-key snapshot batch scan, primary-key delete, and prefix lookup against a real Fluss deployment, using the upstream Java client semantics as the reference for overlapping behaviors.

Legend:

- `✔️` implemented and usable
- `~` in progress / partial / WIP
- blank not implemented yet

## Data Operations

These operations live under the table append, scan, upsert, and lookup surfaces.

| Table Type | Operation | Java Client | Go Client |
|------------|-----------|-------------|-----------|
| Log | Append | ✔️ | ✔️ |
| Log | Typed Append | ✔️ | ✔️ |
| Log | Scan | ✔️ | ✔️ |
| Log | Scan with Projection | ✔️ | ✔️ |
| Log | Typed Scan | ✔️ | ✔️ |
| Log | Batch Scan with Limit | ✔️ | ✔️ |
| Primary Key | Upsert | ✔️ | ✔️ |
| Primary Key | Upsert with Partial Update | ✔️ | ✔️ |
| Primary Key | Typed Upsert | ✔️ | ✔️ |
| Primary Key | Delete | ✔️ | ✔️ |
| Primary Key | Lookup | ✔️ | ✔️ |
| Primary Key | Prefix Lookup | ✔️ | ✔️ |
| Primary Key | Typed Lookup | ✔️ | ✔️ |
| Primary Key | Batch Scan with Limit | ✔️ | ✔️ |
| Primary Key | Batch Scan (Snapshot) | ✔️ | ✔️ |

For more details, see [Table Overview](https://fluss.apache.org/docs/table-design/overview).

Current note for primary-key tables:
- the canonical real-cluster E2E now validates the Go client against Fluss `COMPACTED` KV table semantics for upsert, partial update, lookup, limit scan, delete, and prefix lookup
- these flows are intentionally aligned to the overlapping upstream Java client behavior, then adapted to the Go-native public API surface
- the Go client now exercises primary-key snapshot batch scan in the canonical Fluss+Paimon harness using the same upstream-aligned snapshot metadata flow plus local read-only snapshot iteration
- the Go snapshot path now includes schema-id-aware remapping for older snapshot rows onto the requested target schema
- real-cluster compatibility is proven for the current RustFS/Paimon demo path, and the built-in fetcher now supports both `s3://` and local `file://`/plain-path sources; broader snapshot-layout portability across additional filesystem backends and cluster variants still deserves follow-up validation

Current note for log projection:
- upstream/server behavior only supports column projection for `ARROW` log format
- the Go client now has an Arrow append/decode path plus a projection-aware `FetchLogWithOptions(...)` request path
- the real-cluster support-contract demo validates projection against a dedicated `ARROW` log table while keeping the broader all-types harness on `INDEXED` tables

## Data Types

| DataType | Java Client | Go Client |
|----------|-------------|-----------|
| BOOLEAN | ✔️ | ✔️ |
| TINYINT | ✔️ | ✔️ |
| SMALLINT | ✔️ | ✔️ |
| INT | ✔️ | ✔️ |
| BIGINT | ✔️ | ✔️ |
| FLOAT | ✔️ | ✔️ |
| DOUBLE | ✔️ | ✔️ |
| CHAR(n) | ✔️ | ✔️ |
| STRING | ✔️ | ✔️ |
| DECIMAL(p, s) | ✔️ | ✔️ |
| DATE | ✔️ | ✔️ |
| TIME | ✔️ | ✔️ |
| TIME(p) | ✔️ | ✔️ |
| TIMESTAMP | ✔️ | ✔️ |
| TIMESTAMP(p) | ✔️ | ✔️ |
| TIMESTAMP_LTZ | ✔️ | ✔️ |
| TIMESTAMP_LTZ(p) | ✔️ | ✔️ |
| BINARY(n) | ✔️ | ✔️ |
| BYTES | ✔️ | ✔️ |
| ARRAY<t> | ✔️ | ✔️ |
| MAP<kt, vt> | ✔️ | ✔️ |
| ROW<n0 t0, n1 t1, ...> | ✔️ | ✔️ |

For more details, see [Data Types](https://fluss.apache.org/docs/table-design/data-types).

## Admin Operations

Current note for admin operations:
- the canonical Fluss+Paimon real-cluster harness is now green for safe database, table, and partition lifecycle coverage through the public Go admin API
- cluster-global admin mutations such as ACL changes, cluster config mutation, rebalance control, and server-tag mutation are implemented in the Go SDK and covered by mock integration tests, but they are intentionally outside the single-tablet demo contract for now

| Entity | Operation | Java Client | Go Client |
|--------|-----------|-------------|-----------|
| Database | CreateDatabase | ✔️ | ✔️ |
| Database | DropDatabase | ✔️ | ✔️ |
| Database | DatabaseExists | ✔️ | ✔️ |
| Database | GetDatabaseInfo | ✔️ | ✔️ |
| Database | ListDatabases | ✔️ | ✔️ |
| Table | AlterTable | ✔️ | ✔️ |
| Table | CreateTable | ✔️ | ✔️ |
| Table | DropTable | ✔️ | ✔️ |
| Table | GetTableSchema | ✔️ | ✔️ |
| Table | GetTableInfo | ✔️ | ✔️ |
| Table | ListTables | ✔️ | ✔️ |
| Partition | CreatePartition | ✔️ | ✔️ |
| Partition | DropPartition | ✔️ | ✔️ |
| Partition | ListPartitionInfos | ✔️ | ✔️ |
| Snapshot | GetKvSnapshotMetadata | ✔️ | ✔️ |
| Snapshot | GetLatestKvSnapshots | ✔️ | ✔️ |
| Snapshot | GetLatestLakeSnapshot | ✔️ | ✔️ |
| Bucket | ListOffsets | ✔️ | ✔️ |
| Cluster | AlterClusterConfigs | ✔️ | ✔️ |
| Cluster | DescribeClusterConfigs | ✔️ | ✔️ |
| Cluster | CancelRebalance | ✔️ | ✔️ |
| Cluster | Rebalance | ✔️ | ✔️ |
| Cluster | ListRebalanceProgress | ✔️ | ✔️ |
| Server | AddServerTag | ✔️ | ✔️ |
| Server | RemoveServerTag | ✔️ | ✔️ |
| ACL | CreateAcls | ✔️ | ✔️ |
| ACL | DropAcls | ✔️ | ✔️ |
| ACL | ListAcls | ✔️ | ✔️ |

## Data Lake Formats

| Format | Java Client | Go Client |
|--------|-------------|-----------|
| Iceberg | ✔️ |  |
| Lance | ✔️ |  |
| Paimon | ✔️ | ✔️ |

For Go, `Paimon` here means the SDK is validated against a Fluss deployment configured with Paimon-backed lakehouse infrastructure. It does not imply additional lake-specific public APIs beyond the flows currently implemented and exercised.

For more details, see [Streaming Lakehouse](https://fluss.apache.org/docs/streaming-lakehouse/overview).
