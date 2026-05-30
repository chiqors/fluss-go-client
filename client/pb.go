package client

import (
	"fmt"

	"github.com/chiqors/fluss-go-client/internal/metadata"
	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"google.golang.org/protobuf/proto"
)

func buildTablePath(path TablePath) *flusspb.PbTablePath {
	return &flusspb.PbTablePath{
		DatabaseName: proto.String(path.DatabaseName),
		TableName:    proto.String(path.TableName),
	}
}

func buildProduceBucketRecord(partitionID *int64, bucketID int32, payload []byte) *flusspb.PbProduceLogReqForBucket {
	msg := &flusspb.PbProduceLogReqForBucket{
		BucketId: proto.Int32(bucketID),
		Records:  payload,
	}
	if partitionID != nil {
		msg.PartitionId = proto.Int64(*partitionID)
	}
	return msg
}

func buildPutBucketRecord(partitionID *int64, bucketID int32, payload []byte) *flusspb.PbPutKvReqForBucket {
	msg := &flusspb.PbPutKvReqForBucket{
		BucketId: proto.Int32(bucketID),
		Records:  payload,
	}
	if partitionID != nil {
		msg.PartitionId = proto.Int64(*partitionID)
	}
	return msg
}

func buildLookupBucket(req LookupBucketRequest) *flusspb.PbLookupReqForBucket {
	msg := &flusspb.PbLookupReqForBucket{
		BucketId: proto.Int32(req.BucketID),
		Keys:     append([][]byte(nil), req.Keys...),
	}
	if req.PartitionID != nil {
		msg.PartitionId = proto.Int64(*req.PartitionID)
	}
	return msg
}

func buildPrefixLookupBucket(req LookupBucketRequest) *flusspb.PbPrefixLookupReqForBucket {
	msg := &flusspb.PbPrefixLookupReqForBucket{
		BucketId: proto.Int32(req.BucketID),
		Keys:     append([][]byte(nil), req.Keys...),
	}
	if req.PartitionID != nil {
		msg.PartitionId = proto.Int64(*req.PartitionID)
	}
	return msg
}

func buildFetchLogBucket(req FetchBucketRequest) *flusspb.PbFetchLogReqForBucket {
	msg := &flusspb.PbFetchLogReqForBucket{
		BucketId:      proto.Int32(req.BucketID),
		FetchOffset:   proto.Int64(req.FetchOffset),
		MaxFetchBytes: proto.Int32(req.MaxFetchBytes),
	}
	if req.PartitionID != nil {
		msg.PartitionId = proto.Int64(*req.PartitionID)
	}
	return msg
}

func parseServerNode(node *flusspb.PbServerNode) metadata.ServerNode {
	out := metadata.ServerNode{
		ID:   node.GetNodeId(),
		Host: node.GetHost(),
		Port: node.GetPort(),
	}
	if node.Listeners != nil {
		out.Listeners = node.GetListeners()
	}
	if node.Rack != nil {
		out.Rack = node.GetRack()
	}
	return out
}

func parseTableMetadata(msg *flusspb.PbTableMetadata) (metadata.TableInfo, []metadata.BucketRoute, error) {
	path, err := parseTablePath(msg.GetTablePath())
	if err != nil {
		return metadata.TableInfo{}, nil, err
	}
	info := metadata.TableInfo{
		Path:         path,
		ID:           msg.GetTableId(),
		SchemaID:     msg.GetSchemaId(),
		TableJSON:    append([]byte(nil), msg.GetTableJson()...),
		CreatedTime:  msg.GetCreatedTime(),
		ModifiedTime: msg.GetModifiedTime(),
	}
	var routes []metadata.BucketRoute
	for _, bucket := range msg.GetBucketMetadata() {
		route, err := parseBucketMetadata(info.ID, nil, bucket)
		if err != nil {
			return metadata.TableInfo{}, nil, err
		}
		routes = append(routes, route)
	}
	return info, routes, nil
}

func parsePartitionMetadata(msg *flusspb.PbPartitionMetadata) ([]metadata.BucketRoute, error) {
	routes := make([]metadata.BucketRoute, 0, len(msg.GetBucketMetadata()))
	tableID := msg.GetTableId()
	partitionID := msg.GetPartitionId()
	for _, bucket := range msg.GetBucketMetadata() {
		pid := partitionID
		route, err := parseBucketMetadata(tableID, &pid, bucket)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func parseBucketMetadata(tableID int64, partitionID *int64, msg *flusspb.PbBucketMetadata) (metadata.BucketRoute, error) {
	route := metadata.BucketRoute{
		TableID:      tableID,
		BucketID:     msg.GetBucketId(),
		LeaderID:     msg.GetLeaderId(),
		HasPartition: partitionID != nil,
		Replicas:     append([]int32(nil), msg.GetReplicaId()...),
	}
	if partitionID != nil {
		route.PartitionID = *partitionID
	}
	if msg.LeaderEpoch != nil {
		route.LeaderEpoch = msg.GetLeaderEpoch()
	}
	if msg.LeaderId == nil {
		return metadata.BucketRoute{}, fmt.Errorf("leader missing for table=%d bucket=%d", tableID, route.BucketID)
	}
	return route, nil
}

func parseTablePath(msg *flusspb.PbTablePath) (TablePath, error) {
	if msg == nil {
		return TablePath{}, fmt.Errorf("nil table path")
	}
	return TablePath{
		DatabaseName: msg.GetDatabaseName(),
		TableName:    msg.GetTableName(),
	}, nil
}
