package client

import (
	"context"
	"fmt"

	"github.com/chiqors/fluss-go-client/internal/metadata"
	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"google.golang.org/protobuf/proto"
)

type AdminClient struct {
	client *Client
}

func (a *AdminClient) ListDatabases(ctx context.Context, includeSummary bool) ([]string, []DatabaseSummary, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_ListDatabases, "ListDatabasesRequest", "ListDatabasesResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.ListDatabasesRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected list databases request type %T", msg)
		}
		req.IncludeSummary = proto.Bool(includeSummary)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	r, ok := resp.(*flusspb.ListDatabasesResponse)
	if !ok {
		return nil, nil, fmt.Errorf("fluss: unexpected list databases response type %T", resp)
	}
	names := append([]string(nil), r.GetDatabaseName()...)
	summaries := make([]DatabaseSummary, 0, len(r.GetDatabaseSummary()))
	for _, item := range r.GetDatabaseSummary() {
		summaries = append(summaries, DatabaseSummary{
			DatabaseName: item.GetDatabaseName(),
			CreatedTime:  item.GetCreatedTime(),
			TableCount:   item.GetTableCount(),
		})
	}
	return names, summaries, nil
}

func (a *AdminClient) DatabaseExists(ctx context.Context, name string) (bool, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_DatabaseExists, "DatabaseExistsRequest", "DatabaseExistsResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.DatabaseExistsRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected database exists request type %T", msg)
		}
		req.DatabaseName = proto.String(name)
		return nil
	})
	if err != nil {
		return false, err
	}
	r, ok := resp.(*flusspb.DatabaseExistsResponse)
	if !ok {
		return false, fmt.Errorf("fluss: unexpected database exists response type %T", resp)
	}
	return r.GetExists(), nil
}

func (a *AdminClient) CreateDatabase(ctx context.Context, name string, databaseJSON []byte, ignoreIfExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_CreateDatabase, "CreateDatabaseRequest", "CreateDatabaseResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.CreateDatabaseRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected create database request type %T", msg)
		}
		req.DatabaseName = proto.String(name)
		req.IgnoreIfExists = proto.Bool(ignoreIfExists)
		if len(databaseJSON) > 0 {
			req.DatabaseJson = append([]byte(nil), databaseJSON...)
		}
		return nil
	})
	return err
}

func (a *AdminClient) DropDatabase(ctx context.Context, name string, ignoreIfNotExists, cascade bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_DropDatabase, "DropDatabaseRequest", "DropDatabaseResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.DropDatabaseRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected drop database request type %T", msg)
		}
		req.DatabaseName = proto.String(name)
		req.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
		req.Cascade = proto.Bool(cascade)
		return nil
	})
	return err
}

func (a *AdminClient) GetDatabaseInfo(ctx context.Context, name string) (DatabaseInfo, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetDatabaseInfo, "GetDatabaseInfoRequest", "GetDatabaseInfoResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetDatabaseInfoRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get database info request type %T", msg)
		}
		req.DatabaseName = proto.String(name)
		return nil
	})
	if err != nil {
		return DatabaseInfo{}, err
	}
	r, ok := resp.(*flusspb.GetDatabaseInfoResponse)
	if !ok {
		return DatabaseInfo{}, fmt.Errorf("fluss: unexpected get database info response type %T", resp)
	}
	return DatabaseInfo{
		JSON:         append([]byte(nil), r.GetDatabaseJson()...),
		CreatedTime:  r.GetCreatedTime(),
		ModifiedTime: r.GetModifiedTime(),
	}, nil
}

func (a *AdminClient) ListTables(ctx context.Context, database string) ([]string, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_ListTables, "ListTablesRequest", "ListTablesResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.ListTablesRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected list tables request type %T", msg)
		}
		req.DatabaseName = proto.String(database)
		return nil
	})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*flusspb.ListTablesResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected list tables response type %T", resp)
	}
	return append([]string(nil), r.GetTableName()...), nil
}

func (a *AdminClient) TableExists(ctx context.Context, path TablePath) (bool, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_TableExists, "TableExistsRequest", "TableExistsResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.TableExistsRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected table exists request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		return nil
	})
	if err != nil {
		return false, err
	}
	r, ok := resp.(*flusspb.TableExistsResponse)
	if !ok {
		return false, fmt.Errorf("fluss: unexpected table exists response type %T", resp)
	}
	return r.GetExists(), nil
}

func (a *AdminClient) CreateTable(ctx context.Context, path TablePath, tableJSON []byte, ignoreIfExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_CreateTable, "CreateTableRequest", "CreateTableResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.CreateTableRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected create table request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		req.TableJson = append([]byte(nil), tableJSON...)
		req.IgnoreIfExists = proto.Bool(ignoreIfExists)
		return nil
	})
	return err
}

func (a *AdminClient) AlterTable(ctx context.Context, path TablePath, changes []AlterTableChange, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_AlterTable, "AlterTableRequest", "AlterTableResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.AlterTableRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected alter table request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		req.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
		for _, change := range changes {
			switch v := change.(type) {
			case TableConfigChange:
				req.ConfigChanges = append(req.ConfigChanges, buildAlterConfig(v))
			case AddColumnChange:
				req.AddColumns = append(req.AddColumns, buildAddColumn(v))
			case DropColumnChange:
				req.DropColumns = append(req.DropColumns, &flusspb.PbDropColumn{
					ColumnName: proto.String(v.ColumnName),
				})
			case RenameColumnChange:
				req.RenameColumns = append(req.RenameColumns, &flusspb.PbRenameColumn{
					OldColumnName: proto.String(v.OldColumnName),
					NewColumnName: proto.String(v.NewColumnName),
				})
			case ModifyColumnChange:
				req.ModifyColumns = append(req.ModifyColumns, buildModifyColumn(v))
			default:
				return fmt.Errorf("fluss: unsupported alter table change type %T", change)
			}
		}
		return nil
	})
	return err
}

func (a *AdminClient) DropTable(ctx context.Context, path TablePath, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_DropTable, "DropTableRequest", "DropTableResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.DropTableRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected drop table request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		req.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
		return nil
	})
	return err
}

func (a *AdminClient) GetTableInfo(ctx context.Context, path TablePath) (TableInfo, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetTableInfo, "GetTableInfoRequest", "GetTableInfoResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetTableInfoRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get table info request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		return nil
	})
	if err != nil {
		return TableInfo{}, err
	}
	r, ok := resp.(*flusspb.GetTableInfoResponse)
	if !ok {
		return TableInfo{}, fmt.Errorf("fluss: unexpected get table info response type %T", resp)
	}
	info := TableInfo{
		Path:         path,
		ID:           r.GetTableId(),
		SchemaID:     r.GetSchemaId(),
		JSON:         append([]byte(nil), r.GetTableJson()...),
		CreatedTime:  r.GetCreatedTime(),
		ModifiedTime: r.GetModifiedTime(),
	}
	a.client.metadata.SetTable(metadata.TableInfo{
		Path:         path,
		ID:           info.ID,
		SchemaID:     info.SchemaID,
		TableJSON:    info.JSON,
		CreatedTime:  info.CreatedTime,
		ModifiedTime: info.ModifiedTime,
	})
	return info, nil
}

func (a *AdminClient) GetTableSchema(ctx context.Context, path TablePath, schemaID *int32) (SchemaInfo, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetTableSchema, "GetTableSchemaRequest", "GetTableSchemaResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetTableSchemaRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get table schema request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		if schemaID != nil {
			req.SchemaId = proto.Int32(*schemaID)
		}
		return nil
	})
	if err != nil {
		return SchemaInfo{}, err
	}
	r, ok := resp.(*flusspb.GetTableSchemaResponse)
	if !ok {
		return SchemaInfo{}, fmt.Errorf("fluss: unexpected get table schema response type %T", resp)
	}
	return SchemaInfo{
		SchemaID: r.GetSchemaId(),
		JSON:     append([]byte(nil), r.GetSchemaJson()...),
	}, nil
}

func (a *AdminClient) ListPartitionInfos(ctx context.Context, path TablePath) ([]PartitionInfo, error) {
	return a.ListPartitionInfosWithSpec(ctx, path, nil)
}

func (a *AdminClient) ListPartitionInfosWithSpec(ctx context.Context, path TablePath, spec PartitionSpec) ([]PartitionInfo, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_ListPartitionInfos, "ListPartitionInfosRequest", "ListPartitionInfosResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.ListPartitionInfosRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected list partition infos request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		if spec != nil {
			req.PartialPartitionSpec = buildPartitionSpec(spec)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*flusspb.ListPartitionInfosResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected list partition infos response type %T", resp)
	}
	out := make([]PartitionInfo, 0, len(r.GetPartitionsInfo()))
	for _, item := range r.GetPartitionsInfo() {
		info := PartitionInfo{PartitionID: item.GetPartitionId()}
		if spec := item.GetPartitionSpec(); spec != nil {
			for _, kv := range spec.GetPartitionKeyValues() {
				info.PartitionSpec = append(info.PartitionSpec, PartitionKV{
					Key:   kv.GetKey(),
					Value: kv.GetValue(),
				})
			}
		}
		info.RemoteDataDir = item.GetRemoteDataDir()
		out = append(out, info)
	}
	return out, nil
}

func (a *AdminClient) CreatePartition(ctx context.Context, path TablePath, spec PartitionSpec, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_CreatePartition, "CreatePartitionRequest", "CreatePartitionResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.CreatePartitionRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected create partition request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		req.PartitionSpec = buildPartitionSpec(spec)
		req.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
		return nil
	})
	return err
}

func (a *AdminClient) DropPartition(ctx context.Context, path TablePath, spec PartitionSpec, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, flusspb.ApiKey_DropPartition, "DropPartitionRequest", "DropPartitionResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.DropPartitionRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected drop partition request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		req.PartitionSpec = buildPartitionSpec(spec)
		req.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
		return nil
	})
	return err
}

func (a *AdminClient) GetLatestKvSnapshots(ctx context.Context, path TablePath, partitionName *string) (KvSnapshots, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetLatestKvSnapshots, "GetLatestKvSnapshotsRequest", "GetLatestKvSnapshotsResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetLatestKvSnapshotsRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get latest kv snapshots request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		if partitionName != nil {
			req.PartitionName = proto.String(*partitionName)
		}
		return nil
	})
	if err != nil {
		return KvSnapshots{}, err
	}
	r, ok := resp.(*flusspb.GetLatestKvSnapshotsResponse)
	if !ok {
		return KvSnapshots{}, fmt.Errorf("fluss: unexpected get latest kv snapshots response type %T", resp)
	}
	out := KvSnapshots{
		TableID:     r.GetTableId(),
		SnapshotIDs: make(map[int32]*int64, len(r.GetLatestSnapshots())),
		LogOffsets:  make(map[int32]*int64, len(r.GetLatestSnapshots())),
	}
	if r.PartitionId != nil {
		partitionID := r.GetPartitionId()
		out.PartitionID = &partitionID
	}
	for _, snapshot := range r.GetLatestSnapshots() {
		bucketID := snapshot.GetBucketId()
		if snapshot.SnapshotId != nil {
			snapshotID := snapshot.GetSnapshotId()
			out.SnapshotIDs[bucketID] = &snapshotID
		} else {
			out.SnapshotIDs[bucketID] = nil
		}
		if snapshot.LogOffset != nil {
			logOffset := snapshot.GetLogOffset()
			out.LogOffsets[bucketID] = &logOffset
		} else {
			out.LogOffsets[bucketID] = nil
		}
	}
	return out, nil
}

func (a *AdminClient) GetKvSnapshotMetadata(ctx context.Context, tableID int64, partitionID *int64, bucketID int32, snapshotID int64) (KvSnapshotMetadata, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetKvSnapshotMetadata, "GetKvSnapshotMetadataRequest", "GetKvSnapshotMetadataResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetKvSnapshotMetadataRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get kv snapshot metadata request type %T", msg)
		}
		req.TableId = proto.Int64(tableID)
		if partitionID != nil {
			req.PartitionId = proto.Int64(*partitionID)
		}
		req.BucketId = proto.Int32(bucketID)
		req.SnapshotId = proto.Int64(snapshotID)
		return nil
	})
	if err != nil {
		return KvSnapshotMetadata{}, err
	}
	r, ok := resp.(*flusspb.GetKvSnapshotMetadataResponse)
	if !ok {
		return KvSnapshotMetadata{}, fmt.Errorf("fluss: unexpected get kv snapshot metadata response type %T", resp)
	}
	out := KvSnapshotMetadata{
		LogOffset:     r.GetLogOffset(),
		SnapshotFiles: make([]SnapshotFile, 0, len(r.GetSnapshotFiles())),
	}
	for _, file := range r.GetSnapshotFiles() {
		out.SnapshotFiles = append(out.SnapshotFiles, SnapshotFile{
			RemotePath:    file.GetRemotePath(),
			LocalFileName: file.GetLocalFileName(),
		})
	}
	return out, nil
}

func (a *AdminClient) GetLatestLakeSnapshot(ctx context.Context, path TablePath) (LakeSnapshot, error) {
	return a.getLakeSnapshot(ctx, path, nil, nil)
}

func (a *AdminClient) GetLakeSnapshot(ctx context.Context, path TablePath, snapshotID int64) (LakeSnapshot, error) {
	return a.getLakeSnapshot(ctx, path, &snapshotID, nil)
}

func (a *AdminClient) GetReadableLakeSnapshot(ctx context.Context, path TablePath) (LakeSnapshot, error) {
	readable := true
	return a.getLakeSnapshot(ctx, path, nil, &readable)
}

func (a *AdminClient) getLakeSnapshot(ctx context.Context, path TablePath, snapshotID *int64, readable *bool) (LakeSnapshot, error) {
	resp, err := a.invokeAny(ctx, flusspb.ApiKey_GetLakeSnapshot, "GetLakeSnapshotRequest", "GetLakeSnapshotResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.GetLakeSnapshotRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected get lake snapshot request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
		if snapshotID != nil {
			req.SnapshotId = proto.Int64(*snapshotID)
		}
		if readable != nil {
			req.Readable = proto.Bool(*readable)
		}
		return nil
	})
	if err != nil {
		return LakeSnapshot{}, err
	}
	r, ok := resp.(*flusspb.GetLakeSnapshotResponse)
	if !ok {
		return LakeSnapshot{}, fmt.Errorf("fluss: unexpected get lake snapshot response type %T", resp)
	}
	out := LakeSnapshot{
		TableID:    r.GetTableId(),
		SnapshotID: r.GetSnapshotId(),
		Buckets:    make([]LakeSnapshotBucket, 0, len(r.GetBucketSnapshots())),
	}
	for _, bucket := range r.GetBucketSnapshots() {
		item := LakeSnapshotBucket{
			BucketID:  bucket.GetBucketId(),
			LogOffset: bucket.GetLogOffset(),
		}
		if bucket.PartitionId != nil {
			partitionID := bucket.GetPartitionId()
			item.PartitionID = &partitionID
		}
		out.Buckets = append(out.Buckets, item)
	}
	return out, nil
}

func (a *AdminClient) invokeAny(ctx context.Context, api flusspb.ApiKey, reqName, respName string, build func(proto.Message) error) (proto.Message, error) {
	addr := a.client.endpoints[0]
	if coordinator, ok := a.client.metadata.Coordinator(); ok {
		addr = coordinator.Address()
	}
	msg, err := a.client.rpc.Invoke(ctx, addr, api, reqName, respName, build)
	if err != nil {
		return nil, err
	}
	resp, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected proto response type %T", msg)
	}
	return resp, nil
}

func (a *AdminClient) invokeCoordinator(ctx context.Context, api flusspb.ApiKey, reqName, respName string, build func(proto.Message) error) (proto.Message, error) {
	if _, ok := a.client.metadata.Coordinator(); !ok {
		if err := a.client.RefreshMetadata(ctx, nil, nil); err != nil {
			return nil, err
		}
	}
	return a.invokeAny(ctx, api, reqName, respName, build)
}

func buildPartitionSpec(spec PartitionSpec) *flusspb.PbPartitionSpec {
	if spec == nil {
		return nil
	}
	out := &flusspb.PbPartitionSpec{
		PartitionKeyValues: make([]*flusspb.PbKeyValue, 0, len(spec)),
	}
	for _, kv := range spec {
		out.PartitionKeyValues = append(out.PartitionKeyValues, &flusspb.PbKeyValue{
			Key:   proto.String(kv.Key),
			Value: proto.String(kv.Value),
		})
	}
	return out
}

func buildAlterConfig(change TableConfigChange) *flusspb.PbAlterConfig {
	out := &flusspb.PbAlterConfig{
		ConfigKey: proto.String(change.Key),
		OpType:    proto.Int32(int32(change.Op)),
	}
	if change.Value != nil {
		out.ConfigValue = proto.String(*change.Value)
	}
	return out
}

func buildAddColumn(change AddColumnChange) *flusspb.PbAddColumn {
	out := &flusspb.PbAddColumn{
		ColumnName:         proto.String(change.ColumnName),
		DataTypeJson:       append([]byte(nil), change.DataTypeJSON...),
		ColumnPositionType: proto.Int32(int32(change.ColumnPositionType)),
	}
	if change.Comment != nil {
		out.Comment = proto.String(*change.Comment)
	}
	return out
}

func buildModifyColumn(change ModifyColumnChange) *flusspb.PbModifyColumn {
	out := &flusspb.PbModifyColumn{
		ColumnName: proto.String(change.ColumnName),
	}
	if change.DataTypeJSON != nil {
		out.DataTypeJson = append([]byte(nil), change.DataTypeJSON...)
	}
	if change.Comment != nil {
		out.Comment = proto.String(*change.Comment)
	}
	if change.ColumnPositionType != nil {
		out.ColumnPositionType = proto.Int32(int32(*change.ColumnPositionType))
	}
	return out
}
