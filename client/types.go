package client

import (
	"errors"

	"github.com/chiqors/fluss-go-client/internal/metadata"
)

var ErrClosed = errors.New("fluss: resource is closed")
var ErrBufferFull = errors.New("fluss: writer buffer is full")

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

type PartitionSpec []PartitionKV

type PartitionInfo struct {
	PartitionID   int64
	PartitionSpec []PartitionKV
	RemoteDataDir string
}

type AlterConfigOp int32

const (
	AlterConfigSet      AlterConfigOp = 0
	AlterConfigDelete   AlterConfigOp = 1
	AlterConfigAppend   AlterConfigOp = 2
	AlterConfigSubtract AlterConfigOp = 3
)

type ColumnPositionType int32

const (
	ColumnPositionLast  ColumnPositionType = 0
	ColumnPositionFirst ColumnPositionType = 1
	ColumnPositionAfter ColumnPositionType = 3
)

type AlterTableChange interface {
	alterTableChange()
}

type TableConfigChange struct {
	Key   string
	Value *string
	Op    AlterConfigOp
}

func (TableConfigChange) alterTableChange() {}

type AddColumnChange struct {
	ColumnName         string
	DataTypeJSON       []byte
	Comment            *string
	ColumnPositionType ColumnPositionType
}

func (AddColumnChange) alterTableChange() {}

type DropColumnChange struct {
	ColumnName string
}

func (DropColumnChange) alterTableChange() {}

type RenameColumnChange struct {
	OldColumnName string
	NewColumnName string
}

func (RenameColumnChange) alterTableChange() {}

type ModifyColumnChange struct {
	ColumnName         string
	DataTypeJSON       []byte
	Comment            *string
	ColumnPositionType *ColumnPositionType
}

func (ModifyColumnChange) alterTableChange() {}

type ClusterConfigEntry struct {
	Key    string
	Value  *string
	Source string
}

type ServerTag int32

type RebalanceGoal int32

type ACLBinding struct {
	ResourceName   string
	ResourceType   int32
	PrincipalName  string
	PrincipalType  string
	Host           string
	OperationType  int32
	PermissionType int32
}

type ACLFilter struct {
	ResourceName   *string
	ResourceType   int32
	PrincipalName  *string
	PrincipalType  *string
	Host           *string
	OperationType  int32
	PermissionType int32
}

type CreateACLResult struct {
	ACL          ACLBinding
	ErrorCode    *int32
	ErrorMessage *string
}

type DropACLMatchingResult struct {
	ACL          ACLBinding
	ErrorCode    *int32
	ErrorMessage *string
}

type DropACLFilterResult struct {
	MatchingACLs []DropACLMatchingResult
	ErrorCode    *int32
	ErrorMessage *string
}

type RebalanceBucketPlan struct {
	PartitionID      *int64
	BucketID         int32
	OriginalLeader   *int32
	NewLeader        *int32
	OriginalReplicas []int32
	NewReplicas      []int32
}

type RebalanceBucketProgress struct {
	Plan   RebalanceBucketPlan
	Status int32
}

type RebalanceTableProgress struct {
	TableID int64
	Buckets []RebalanceBucketProgress
}

type RebalanceProgress struct {
	RebalanceID string
	Status      *int32
	Tables      []RebalanceTableProgress
}

type SnapshotFile struct {
	RemotePath    string
	LocalFileName string
}

type KvSnapshots struct {
	TableID     int64
	PartitionID *int64
	SnapshotIDs map[int32]*int64
	LogOffsets  map[int32]*int64
}

type KvSnapshotMetadata struct {
	LogOffset     int64
	SnapshotFiles []SnapshotFile
}

type SnapshotScanOptions struct {
	PartitionName *string
	PartitionID   *int64
	BucketID      int32
	SnapshotID    *int64
}

type LakeSnapshotBucket struct {
	PartitionID *int64
	BucketID    int32
	LogOffset   int64
}

type LakeSnapshot struct {
	TableID    int64
	SnapshotID int64
	Buckets    []LakeSnapshotBucket
}

type BucketRecordBatch struct {
	PartitionID *int64
	BucketID    int32
	Records     []byte
}

type AppendOptions struct {
	Acks               int32
	TimeoutMs          int32
	MaxBufferedBatches int
	FlushOnClose       *bool
}

type UpsertOptions struct {
	Acks               int32
	TimeoutMs          int32
	TargetColumns      []int32
	AggMode            *int32
	MaxBufferedBatches int
	FlushOnClose       *bool
}

type ProduceResult struct {
	PartitionID  *int64
	BucketID     int32
	BaseOffset   int64
	ErrorCode    *int32
	ErrorMessage *string
}

type PutResult struct {
	PartitionID  *int64
	BucketID     int32
	LogEndOffset int64
	ErrorCode    *int32
	ErrorMessage *string
}

type BucketWriteError struct {
	PartitionID *int64
	BucketID    int32
	Err         error
}

type PartialWriteError struct {
	Operation string
	Failures  []BucketWriteError
}

func (e *PartialWriteError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "fluss: partial write failure"
	}
	return e.Operation + ": partial failure"
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

type FetchLogOptions struct {
	ProjectedFields []int32
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
