package client

import (
	"context"
	"fmt"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/metadata"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

type AdminClient struct {
	client *Client
}

func (a *AdminClient) ListDatabases(ctx context.Context, includeSummary bool) ([]string, []DatabaseSummary, error) {
	resp, err := a.invokeAny(ctx, protocol.ListDatabases, "ListDatabasesRequest", "ListDatabasesResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.DatabaseExists, "DatabaseExistsRequest", "DatabaseExistsResponse", func(msg proto.Message) error {
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
	_, err := a.invokeCoordinator(ctx, protocol.CreateDatabase, "CreateDatabaseRequest", "CreateDatabaseResponse", func(msg proto.Message) error {
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
	_, err := a.invokeCoordinator(ctx, protocol.DropDatabase, "DropDatabaseRequest", "DropDatabaseResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.GetDatabaseInfo, "GetDatabaseInfoRequest", "GetDatabaseInfoResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.ListTables, "ListTablesRequest", "ListTablesResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.TableExists, "TableExistsRequest", "TableExistsResponse", func(msg proto.Message) error {
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
	_, err := a.invokeCoordinator(ctx, protocol.CreateTable, "CreateTableRequest", "CreateTableResponse", func(msg proto.Message) error {
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

func (a *AdminClient) DropTable(ctx context.Context, path TablePath, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, protocol.DropTable, "DropTableRequest", "DropTableResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.GetTableInfo, "GetTableInfoRequest", "GetTableInfoResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.GetTableSchema, "GetTableSchemaRequest", "GetTableSchemaResponse", func(msg proto.Message) error {
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
	resp, err := a.invokeAny(ctx, protocol.ListPartitionInfos, "ListPartitionInfosRequest", "ListPartitionInfosResponse", func(msg proto.Message) error {
		req, ok := msg.(*flusspb.ListPartitionInfosRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected list partition infos request type %T", msg)
		}
		req.TablePath = buildTablePath(path)
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
		out = append(out, info)
	}
	return out, nil
}

func (a *AdminClient) invokeAny(ctx context.Context, api protocol.APIKey, reqName, respName string, build func(proto.Message) error) (proto.Message, error) {
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

func (a *AdminClient) invokeCoordinator(ctx context.Context, api protocol.APIKey, reqName, respName string, build func(proto.Message) error) (proto.Message, error) {
	if _, ok := a.client.metadata.Coordinator(); !ok {
		if err := a.client.RefreshMetadata(ctx, nil, nil); err != nil {
			return nil, err
		}
	}
	return a.invokeAny(ctx, api, reqName, respName, build)
}
