package client

import (
	"context"

	"github.com/chiqors/fluss-go-client/internal/pbutil"
	"github.com/chiqors/fluss-go-client/metadata"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type AdminClient struct {
	client *Client
}

func (a *AdminClient) ListDatabases(ctx context.Context, includeSummary bool) ([]string, []DatabaseSummary, error) {
	resp, err := a.invokeAny(ctx, protocol.ListDatabases, "ListDatabasesRequest", "ListDatabasesResponse", func(msg protoreflect.Message) error {
		return pbutil.SetBool(msg, "include_summary", includeSummary)
	})
	if err != nil {
		return nil, nil, err
	}
	r := resp.ProtoReflect()
	namesField, _ := pbutil.Field(r.Descriptor(), "database_name")
	namesList := r.Get(namesField).List()
	names := make([]string, 0, namesList.Len())
	for i := 0; i < namesList.Len(); i++ {
		names = append(names, namesList.Get(i).String())
	}
	var summaries []DatabaseSummary
	if summariesField := r.Descriptor().Fields().ByName("database_summary"); summariesField != nil {
		list := r.Get(summariesField).List()
		summaries = make([]DatabaseSummary, 0, list.Len())
		for i := 0; i < list.Len(); i++ {
			item := list.Get(i).Message()
			nameField, _ := pbutil.Field(item.Descriptor(), "database_name")
			createdField, _ := pbutil.Field(item.Descriptor(), "created_time")
			countField, _ := pbutil.Field(item.Descriptor(), "table_count")
			summaries = append(summaries, DatabaseSummary{
				DatabaseName: item.Get(nameField).String(),
				CreatedTime:  item.Get(createdField).Int(),
				TableCount:   int32(item.Get(countField).Int()),
			})
		}
	}
	return names, summaries, nil
}

func (a *AdminClient) DatabaseExists(ctx context.Context, name string) (bool, error) {
	resp, err := a.invokeAny(ctx, protocol.DatabaseExists, "DatabaseExistsRequest", "DatabaseExistsResponse", func(msg protoreflect.Message) error {
		return pbutil.SetString(msg, "database_name", name)
	})
	if err != nil {
		return false, err
	}
	field, _ := pbutil.Field(resp.ProtoReflect().Descriptor(), "exists")
	return resp.ProtoReflect().Get(field).Bool(), nil
}

func (a *AdminClient) CreateDatabase(ctx context.Context, name string, databaseJSON []byte, ignoreIfExists bool) error {
	_, err := a.invokeCoordinator(ctx, protocol.CreateDatabase, "CreateDatabaseRequest", "CreateDatabaseResponse", func(msg protoreflect.Message) error {
		if err := pbutil.SetString(msg, "database_name", name); err != nil {
			return err
		}
		if err := pbutil.SetBool(msg, "ignore_if_exists", ignoreIfExists); err != nil {
			return err
		}
		if len(databaseJSON) > 0 {
			return pbutil.SetBytes(msg, "database_json", databaseJSON)
		}
		return nil
	})
	return err
}

func (a *AdminClient) DropDatabase(ctx context.Context, name string, ignoreIfNotExists, cascade bool) error {
	_, err := a.invokeCoordinator(ctx, protocol.DropDatabase, "DropDatabaseRequest", "DropDatabaseResponse", func(msg protoreflect.Message) error {
		if err := pbutil.SetString(msg, "database_name", name); err != nil {
			return err
		}
		if err := pbutil.SetBool(msg, "ignore_if_not_exists", ignoreIfNotExists); err != nil {
			return err
		}
		return pbutil.SetBool(msg, "cascade", cascade)
	})
	return err
}

func (a *AdminClient) GetDatabaseInfo(ctx context.Context, name string) (DatabaseInfo, error) {
	resp, err := a.invokeAny(ctx, protocol.GetDatabaseInfo, "GetDatabaseInfoRequest", "GetDatabaseInfoResponse", func(msg protoreflect.Message) error {
		return pbutil.SetString(msg, "database_name", name)
	})
	if err != nil {
		return DatabaseInfo{}, err
	}
	r := resp.ProtoReflect()
	jsonField, _ := pbutil.Field(r.Descriptor(), "database_json")
	createdField, _ := pbutil.Field(r.Descriptor(), "created_time")
	modifiedField, _ := pbutil.Field(r.Descriptor(), "modified_time")
	return DatabaseInfo{
		JSON:         append([]byte(nil), r.Get(jsonField).Bytes()...),
		CreatedTime:  r.Get(createdField).Int(),
		ModifiedTime: r.Get(modifiedField).Int(),
	}, nil
}

func (a *AdminClient) ListTables(ctx context.Context, database string) ([]string, error) {
	resp, err := a.invokeAny(ctx, protocol.ListTables, "ListTablesRequest", "ListTablesResponse", func(msg protoreflect.Message) error {
		return pbutil.SetString(msg, "database_name", database)
	})
	if err != nil {
		return nil, err
	}
	field, _ := pbutil.Field(resp.ProtoReflect().Descriptor(), "table_name")
	list := resp.ProtoReflect().Get(field).List()
	out := make([]string, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		out = append(out, list.Get(i).String())
	}
	return out, nil
}

func (a *AdminClient) TableExists(ctx context.Context, path TablePath) (bool, error) {
	resp, err := a.invokeAny(ctx, protocol.TableExists, "TableExistsRequest", "TableExistsResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		return pbutil.SetMessage(msg, "table_path", p)
	})
	if err != nil {
		return false, err
	}
	field, _ := pbutil.Field(resp.ProtoReflect().Descriptor(), "exists")
	return resp.ProtoReflect().Get(field).Bool(), nil
}

func (a *AdminClient) CreateTable(ctx context.Context, path TablePath, tableJSON []byte, ignoreIfExists bool) error {
	_, err := a.invokeCoordinator(ctx, protocol.CreateTable, "CreateTableRequest", "CreateTableResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		if err := pbutil.SetMessage(msg, "table_path", p); err != nil {
			return err
		}
		if err := pbutil.SetBytes(msg, "table_json", tableJSON); err != nil {
			return err
		}
		return pbutil.SetBool(msg, "ignore_if_exists", ignoreIfExists)
	})
	return err
}

func (a *AdminClient) DropTable(ctx context.Context, path TablePath, ignoreIfNotExists bool) error {
	_, err := a.invokeCoordinator(ctx, protocol.DropTable, "DropTableRequest", "DropTableResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		if err := pbutil.SetMessage(msg, "table_path", p); err != nil {
			return err
		}
		return pbutil.SetBool(msg, "ignore_if_not_exists", ignoreIfNotExists)
	})
	return err
}

func (a *AdminClient) GetTableInfo(ctx context.Context, path TablePath) (TableInfo, error) {
	resp, err := a.invokeAny(ctx, protocol.GetTableInfo, "GetTableInfoRequest", "GetTableInfoResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		return pbutil.SetMessage(msg, "table_path", p)
	})
	if err != nil {
		return TableInfo{}, err
	}
	r := resp.ProtoReflect()
	tableIDField, _ := pbutil.Field(r.Descriptor(), "table_id")
	schemaIDField, _ := pbutil.Field(r.Descriptor(), "schema_id")
	tableJSONField, _ := pbutil.Field(r.Descriptor(), "table_json")
	createdField, _ := pbutil.Field(r.Descriptor(), "created_time")
	modifiedField, _ := pbutil.Field(r.Descriptor(), "modified_time")
	info := TableInfo{
		Path:         path,
		ID:           r.Get(tableIDField).Int(),
		SchemaID:     int32(r.Get(schemaIDField).Int()),
		JSON:         append([]byte(nil), r.Get(tableJSONField).Bytes()...),
		CreatedTime:  r.Get(createdField).Int(),
		ModifiedTime: r.Get(modifiedField).Int(),
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
	resp, err := a.invokeAny(ctx, protocol.GetTableSchema, "GetTableSchemaRequest", "GetTableSchemaResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		if err := pbutil.SetMessage(msg, "table_path", p); err != nil {
			return err
		}
		if schemaID != nil {
			return pbutil.SetInt32(msg, "schema_id", *schemaID)
		}
		return nil
	})
	if err != nil {
		return SchemaInfo{}, err
	}
	r := resp.ProtoReflect()
	schemaIDField, _ := pbutil.Field(r.Descriptor(), "schema_id")
	jsonField, _ := pbutil.Field(r.Descriptor(), "schema_json")
	return SchemaInfo{
		SchemaID: int32(r.Get(schemaIDField).Int()),
		JSON:     append([]byte(nil), r.Get(jsonField).Bytes()...),
	}, nil
}

func (a *AdminClient) ListPartitionInfos(ctx context.Context, path TablePath) ([]PartitionInfo, error) {
	resp, err := a.invokeAny(ctx, protocol.ListPartitionInfos, "ListPartitionInfosRequest", "ListPartitionInfosResponse", func(msg protoreflect.Message) error {
		p, err := buildTablePath(path)
		if err != nil {
			return err
		}
		return pbutil.SetMessage(msg, "table_path", p)
	})
	if err != nil {
		return nil, err
	}
	field, _ := pbutil.Field(resp.ProtoReflect().Descriptor(), "partitions_info")
	list := resp.ProtoReflect().Get(field).List()
	out := make([]PartitionInfo, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		idField, _ := pbutil.Field(item.Descriptor(), "partition_id")
		specField, _ := pbutil.Field(item.Descriptor(), "partition_spec")
		info := PartitionInfo{PartitionID: item.Get(idField).Int()}
		specMsg := item.Get(specField).Message()
		specListField, _ := pbutil.Field(specMsg.Descriptor(), "partition_key_values")
		specList := specMsg.Get(specListField).List()
		for j := 0; j < specList.Len(); j++ {
			kv := specList.Get(j).Message()
			keyField, _ := pbutil.Field(kv.Descriptor(), "key")
			valueField, _ := pbutil.Field(kv.Descriptor(), "value")
			info.PartitionSpec = append(info.PartitionSpec, PartitionKV{
				Key:   kv.Get(keyField).String(),
				Value: kv.Get(valueField).String(),
			})
		}
		out = append(out, info)
	}
	return out, nil
}

func (a *AdminClient) invokeAny(ctx context.Context, api protocol.APIKey, reqName, respName string, build func(protoreflect.Message) error) (protoreflect.ProtoMessage, error) {
	addr := a.client.endpoints[0]
	if coordinator, ok := a.client.metadata.Coordinator(); ok {
		addr = coordinator.Address()
	}
	return a.client.rpc.Invoke(ctx, addr, api, reqName, respName, func(m *dynamicpb.Message) error {
		return build(m.ProtoReflect())
	})
}

func (a *AdminClient) invokeCoordinator(ctx context.Context, api protocol.APIKey, reqName, respName string, build func(protoreflect.Message) error) (protoreflect.ProtoMessage, error) {
	if _, ok := a.client.metadata.Coordinator(); !ok {
		if err := a.client.RefreshMetadata(ctx, nil, nil); err != nil {
			return nil, err
		}
	}
	return a.invokeAny(ctx, api, reqName, respName, build)
}
