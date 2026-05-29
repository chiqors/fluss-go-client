package client

import (
	"fmt"

	"github.com/chiqors/fluss-go-client/internal/pbutil"
	iproto "github.com/chiqors/fluss-go-client/internal/proto"
	"github.com/chiqors/fluss-go-client/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func buildTablePath(path TablePath) (protoreflect.Message, error) {
	msg, err := iproto.NewMessage("PbTablePath")
	if err != nil {
		return nil, err
	}
	if err := pbutil.SetString(msg.ProtoReflect(), "database_name", path.DatabaseName); err != nil {
		return nil, err
	}
	if err := pbutil.SetString(msg.ProtoReflect(), "table_name", path.TableName); err != nil {
		return nil, err
	}
	return msg.ProtoReflect(), nil
}

func buildBucketRecord(name string, partitionID *int64, bucketID int32, payload []byte) (protoreflect.Message, error) {
	msg, err := iproto.NewMessage(name)
	if err != nil {
		return nil, err
	}
	if partitionID != nil {
		if err := pbutil.SetInt64(msg.ProtoReflect(), "partition_id", *partitionID); err != nil {
			return nil, err
		}
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "bucket_id", bucketID); err != nil {
		return nil, err
	}
	if err := pbutil.SetBytes(msg.ProtoReflect(), "records", payload); err != nil {
		return nil, err
	}
	return msg.ProtoReflect(), nil
}

func pbAppendMessage(msg protoreflect.Message, field string, value protoreflect.Message) error {
	return pbutil.AppendMessage(msg, field, value)
}

func pbAppendInt64(msg protoreflect.Message, field string, values ...int64) error {
	return pbutil.AppendInt64(msg, field, values...)
}

func parseServerNode(msg protoreflect.Message) (metadata.ServerNode, error) {
	nodeIDField, _ := pbutil.Field(msg.Descriptor(), "node_id")
	hostField, _ := pbutil.Field(msg.Descriptor(), "host")
	portField, _ := pbutil.Field(msg.Descriptor(), "port")
	node := metadata.ServerNode{
		ID:   int32(msg.Get(nodeIDField).Int()),
		Host: msg.Get(hostField).String(),
		Port: int32(msg.Get(portField).Int()),
	}
	if listenersField := msg.Descriptor().Fields().ByName("listeners"); listenersField != nil && msg.Has(listenersField) {
		node.Listeners = msg.Get(listenersField).String()
	}
	if rackField := msg.Descriptor().Fields().ByName("rack"); rackField != nil && msg.Has(rackField) {
		node.Rack = msg.Get(rackField).String()
	}
	return node, nil
}

func parseTableMetadata(msg protoreflect.Message) (metadata.TableInfo, []metadata.BucketRoute, error) {
	tablePathField, _ := pbutil.Field(msg.Descriptor(), "table_path")
	tableIDField, _ := pbutil.Field(msg.Descriptor(), "table_id")
	schemaIDField, _ := pbutil.Field(msg.Descriptor(), "schema_id")
	tableJSONField, _ := pbutil.Field(msg.Descriptor(), "table_json")
	createdTimeField, _ := pbutil.Field(msg.Descriptor(), "created_time")
	modifiedTimeField, _ := pbutil.Field(msg.Descriptor(), "modified_time")
	path, err := parseTablePath(msg.Get(tablePathField).Message())
	if err != nil {
		return metadata.TableInfo{}, nil, err
	}
	info := metadata.TableInfo{
		Path:         path,
		ID:           msg.Get(tableIDField).Int(),
		SchemaID:     int32(msg.Get(schemaIDField).Int()),
		TableJSON:    append([]byte(nil), msg.Get(tableJSONField).Bytes()...),
		CreatedTime:  msg.Get(createdTimeField).Int(),
		ModifiedTime: msg.Get(modifiedTimeField).Int(),
	}
	var routes []metadata.BucketRoute
	bucketsField, _ := pbutil.Field(msg.Descriptor(), "bucket_metadata")
	bucketList := msg.Get(bucketsField).List()
	for i := 0; i < bucketList.Len(); i++ {
		route, err := parseBucketMetadata(info.ID, nil, bucketList.Get(i).Message())
		if err != nil {
			return metadata.TableInfo{}, nil, err
		}
		routes = append(routes, route)
	}
	return info, routes, nil
}

func parsePartitionMetadata(msg protoreflect.Message) ([]metadata.BucketRoute, error) {
	tableIDField, _ := pbutil.Field(msg.Descriptor(), "table_id")
	partitionIDField, _ := pbutil.Field(msg.Descriptor(), "partition_id")
	bucketsField, _ := pbutil.Field(msg.Descriptor(), "bucket_metadata")
	tableID := msg.Get(tableIDField).Int()
	partitionID := msg.Get(partitionIDField).Int()
	list := msg.Get(bucketsField).List()
	routes := make([]metadata.BucketRoute, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		pid := partitionID
		route, err := parseBucketMetadata(tableID, &pid, list.Get(i).Message())
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func parseBucketMetadata(tableID int64, partitionID *int64, msg protoreflect.Message) (metadata.BucketRoute, error) {
	bucketIDField, _ := pbutil.Field(msg.Descriptor(), "bucket_id")
	leaderIDField, _ := pbutil.Field(msg.Descriptor(), "leader_id")
	leaderEpochField, _ := pbutil.Field(msg.Descriptor(), "leader_epoch")
	route := metadata.BucketRoute{
		TableID:      tableID,
		BucketID:     int32(msg.Get(bucketIDField).Int()),
		LeaderID:     int32(msg.Get(leaderIDField).Int()),
		HasPartition: partitionID != nil,
	}
	if partitionID != nil {
		route.PartitionID = *partitionID
	}
	if msg.Has(leaderEpochField) {
		route.LeaderEpoch = int32(msg.Get(leaderEpochField).Int())
	}
	replicasField, _ := pbutil.Field(msg.Descriptor(), "replica_id")
	replicas := msg.Get(replicasField).List()
	for i := 0; i < replicas.Len(); i++ {
		route.Replicas = append(route.Replicas, int32(replicas.Get(i).Int()))
	}
	if route.LeaderID == 0 {
		return metadata.BucketRoute{}, fmt.Errorf("leader missing for table=%d bucket=%d", tableID, route.BucketID)
	}
	return route, nil
}

func parseTablePath(msg protoreflect.Message) (TablePath, error) {
	dbField, _ := pbutil.Field(msg.Descriptor(), "database_name")
	tableField, _ := pbutil.Field(msg.Descriptor(), "table_name")
	return TablePath{
		DatabaseName: msg.Get(dbField).String(),
		TableName:    msg.Get(tableField).String(),
	}, nil
}
