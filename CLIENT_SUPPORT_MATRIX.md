# Client Feature Support Matrix

Fluss has a rich set of features and native data types available to users. The tables below summarize what the upstream Java client supports today and what the Go client in this repo supports today.

Legend:

- `✔️` implemented and usable
- `~` in progress / partial / WIP
- blank not implemented yet

## Data Operations

These operations live under the table append, scan, upsert, and lookup surfaces.

| Table Type | Operation | Java Client | Go Client |
|------------|-----------|-------------|-----------|
| Log | Append | ✔️ | ✔️ |
| Log | Typed Append | ✔️ |  |
| Log | Scan | ✔️ | ✔️ |
| Log | Scan with Projection | ✔️ | ~ |
| Log | Typed Scan | ✔️ |  |
| Log | Batch Scan with Limit | ✔️ | ✔️ |
| Primary Key | Upsert | ✔️ | ✔️ |
| Primary Key | Upsert with Partial Update | ✔️ | ✔️ |
| Primary Key | Typed Upsert | ✔️ |  |
| Primary Key | Delete | ✔️ | ✔️ |
| Primary Key | Lookup | ✔️ | ✔️ |
| Primary Key | Prefix Lookup | ✔️ | ✔️ |
| Primary Key | Typed Lookup | ✔️ |  |
| Primary Key | Batch Scan with Limit | ✔️ |  |
| Primary Key | Batch Scan (Snapshot) | ✔️ |  |

For more details, see [Table Overview](https://fluss.apache.org/docs/table-design/data-types/).

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
| DECIMAL(p, s) | ✔️ | ~ |
| DATE | ✔️ | ✔️ |
| TIME | ✔️ | ✔️ |
| TIME(p) | ✔️ | ✔️ |
| TIMESTAMP | ✔️ | ✔️ |
| TIMESTAMP(p) | ✔️ | ✔️ |
| TIMESTAMP_LTZ | ✔️ | ✔️ |
| TIMESTAMP_LTZ(p) | ✔️ | ✔️ |
| BINARY(n) | ✔️ | ✔️ |
| BYTES | ✔️ | ✔️ |
| ARRAY<t> | ✔️ |  |
| MAP<kt, vt> | ✔️ |  |
| ROW<n0 t0, n1 t1, ...> | ✔️ |  |

:::tip
For more details, see [Data Types](table-design/data-types.md).
:::

## Admin Operations

| Entity | Operation | Java Client | Go Client |
|--------|-----------|-------------|-----------|
| Database | CreateDatabase | ✔️ | ✔️ |
| Database | DropDatabase | ✔️ | ✔️ |
| Database | DatabaseExists | ✔️ | ✔️ |
| Database | GetDatabaseInfo | ✔️ | ✔️ |
| Database | ListDatabases | ✔️ | ✔️ |
| Table | AlterTable | ✔️ |  |
| Table | CreateTable | ✔️ | ✔️ |
| Table | DropTable | ✔️ | ✔️ |
| Table | GetTableSchema | ✔️ |  |
| Table | GetTableInfo | ✔️ | ✔️ |
| Table | ListTables | ✔️ | ✔️ |
| Partition | CreatePartition | ✔️ |  |
| Partition | DropPartition | ✔️ |  |
| Partition | ListPartitionInfos | ✔️ |  |
| Snapshot | GetKvSnapshotMetadata | ✔️ |  |
| Snapshot | GetLatestKvSnapshots | ✔️ |  |
| Snapshot | GetLatestLakeSnapshot | ✔️ | ✔️ |
| Bucket | ListOffsets | ✔️ | ✔️ |
| Cluster | AlterClusterConfigs | ✔️ |  |
| Cluster | DescribeClusterConfigs | ✔️ |  |
| Cluster | CancelRebalance | ✔️ |  |
| Cluster | Rebalance | ✔️ |  |
| Cluster | ListRebalanceProgress | ✔️ |  |
| Server | AddServerTag | ✔️ |  |
| Server | RemoveServerTag | ✔️ |  |
| ACL | CreateAcls | ✔️ |  |
| ACL | DropAcls | ✔️ |  |
| ACL | ListAcls | ✔️ |  |

## Data Lake Formats

| Format | Java Client | Go Client |
|--------|-------------|-----------|
| Iceberg | ✔️ |  |
| Lance | ✔️ | ✔️ |
| Paimon | ✔️ |  |

For more details, see [Streaming Lakehouse](https://fluss.apache.org/docs/streaming-lakehouse/overview/).
