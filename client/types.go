package client

import "github.com/fluss-client-go/metadata"

type TablePath = metadata.TablePath

type DatabaseInfo struct {
	JSON         []byte
	CreatedTime  int64
	ModifiedTime int64
}

type DatabaseSummary struct {
	DatabaseName string
	CreatedTime  int64
	TableCount   int32
}

type TableInfo struct {
	Path          TablePath
	ID            int64
	SchemaID      int32
	JSON          []byte
	CreatedTime   int64
	ModifiedTime  int64
	RemoteDataDir string
}

type SchemaInfo struct {
	SchemaID int32
	JSON     []byte
}

type PartitionKV struct {
	Key   string
	Value string
}

type PartitionInfo struct {
	PartitionID   int64
	PartitionSpec []PartitionKV
	RemoteDataDir string
}

type BucketRecordBatch struct {
	PartitionID *int64
	BucketID    int32
	Records     []byte
}

type ProduceResult struct {
	PartitionID *int64
	BucketID    int32
	BaseOffset  int64
}

type PutResult struct {
	PartitionID  *int64
	BucketID     int32
	LogEndOffset int64
}

type LookupBucketRequest struct {
	PartitionID *int64
	BucketID    int32
	Keys        [][]byte
}

type LookupBucketValues struct {
	PartitionID *int64
	BucketID    int32
	Values      [][]byte
}

type PrefixLookupBucketValues struct {
	PartitionID *int64
	BucketID    int32
	Values      [][][]byte
}

type FetchBucketRequest struct {
	PartitionID   *int64
	BucketID      int32
	FetchOffset   int64
	MaxFetchBytes int32
}

type FetchedBucket struct {
	PartitionID       *int64
	BucketID          int32
	HighWatermark     int64
	LogStartOffset    int64
	FilteredEndOffset *int64
	Records           []byte
}

type LimitScanResult struct {
	IsLogTable bool
	Records    []byte
}

type ScanKVResult struct {
	ScannerID      []byte
	HasMoreResults bool
	Records        []byte
	LogOffset      *int64
}
