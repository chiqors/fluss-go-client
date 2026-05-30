package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	arrowcodec "github.com/chiqors/fluss-go-client/internal/codec/arrow"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/internal/snapshot"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
)

type mockFlussServer struct {
	t        *testing.T
	ln       net.Listener
	addr     string
	mu       sync.Mutex
	handlers map[flusspb.ApiKey]func(int32, []byte) ([]byte, error)
}

func newMockFlussServer(t *testing.T) *mockFlussServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &mockFlussServer{
		t:        t,
		ln:       ln,
		addr:     ln.Addr().String(),
		handlers: map[flusspb.ApiKey]func(int32, []byte) ([]byte, error){},
	}
	go s.serve()
	return s
}

func (s *mockFlussServer) Close() {
	_ = s.ln.Close()
}

func (s *mockFlussServer) on(api flusspb.ApiKey, fn func(int32, []byte) ([]byte, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[api] = fn
}

func (s *mockFlussServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *mockFlussServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		apiKey, reqID, payload, err := readRequest(conn)
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
		s.mu.Lock()
		handler := s.handlers[apiKey]
		s.mu.Unlock()
		if handler == nil {
			_, _ = conn.Write(encodeErrorResponse(reqID, -1, fmt.Sprintf("no handler for api %d", apiKey)))
			continue
		}
		resp, err := handler(reqID, payload)
		if err != nil {
			_, _ = conn.Write(encodeErrorResponse(reqID, -1, err.Error()))
			continue
		}
		_, _ = conn.Write(encodeSuccessResponse(reqID, resp))
	}
}

func readRequest(r io.Reader) (flusspb.ApiKey, int32, []byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return 0, 0, nil, err
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, 0, nil, err
	}
	apiKey := flusspb.ApiKey(binary.BigEndian.Uint16(body[0:2]))
	reqID := int32(binary.BigEndian.Uint32(body[4:8]))
	return apiKey, reqID, body[8:], nil
}

func encodeSuccessResponse(reqID int32, payload []byte) []byte {
	frameLen := 5 + len(payload)
	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	buf[4] = byte(flusspb.ResponseType_ResponseSuccess)
	binary.BigEndian.PutUint32(buf[5:9], uint32(reqID))
	copy(buf[9:], payload)
	return buf
}

func encodeErrorResponse(reqID int32, code int32, message string) []byte {
	msg := &flusspb.ErrorResponse{ErrorCode: proto.Int32(code)}
	if message != "" {
		msg.ErrorMessage = proto.String(message)
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	frameLen := 5 + len(payload)
	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	buf[4] = byte(flusspb.ResponseType_ResponseError)
	binary.BigEndian.PutUint32(buf[5:9], uint32(reqID))
	copy(buf[9:], payload)
	return buf
}

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal %T: %v", msg, err)
	}
	return payload
}

func apiVersionsResponse(apis ...flusspb.ApiKey) *flusspb.ApiVersionsResponse {
	resp := &flusspb.ApiVersionsResponse{}
	for _, api := range apis {
		resp.ApiVersions = append(resp.ApiVersions, &flusspb.PbApiVersion{
			ApiKey:     proto.Int32(int32(api)),
			MinVersion: proto.Int32(0),
			MaxVersion: proto.Int32(0),
		})
	}
	return resp
}

func serverNode(nodeID int32, host string, port int32) *flusspb.PbServerNode {
	return &flusspb.PbServerNode{
		NodeId: proto.Int32(nodeID),
		Host:   proto.String(host),
		Port:   proto.Int32(port),
	}
}

func testTablePath(db, table string) *flusspb.PbTablePath {
	return &flusspb.PbTablePath{
		DatabaseName: proto.String(db),
		TableName:    proto.String(table),
	}
}

func metadataResponseForSingleBucket(host string, port int32, path TablePath, tableID int64, schemaID int32) *flusspb.MetadataResponse {
	node := serverNode(1, host, port)
	return &flusspb.MetadataResponse{
		CoordinatorServer: node,
		TabletServers:     []*flusspb.PbServerNode{node},
		TableMetadata: []*flusspb.PbTableMetadata{{
			TablePath:    testTablePath(path.DatabaseName, path.TableName),
			TableId:      proto.Int64(tableID),
			SchemaId:     proto.Int32(schemaID),
			TableJson:    []byte(`{}`),
			CreatedTime:  proto.Int64(1),
			ModifiedTime: proto.Int64(1),
			BucketMetadata: []*flusspb.PbBucketMetadata{{
				BucketId:    proto.Int32(0),
				LeaderId:    proto.Int32(1),
				ReplicaId:   []int32{1},
				LeaderEpoch: proto.Int32(1),
			}},
		}},
	}
}

func metadataResponseForSingleBucketWithJSON(host string, port int32, path TablePath, tableID int64, schemaID int32, tableJSON []byte) *flusspb.MetadataResponse {
	resp := metadataResponseForSingleBucket(host, port, path, tableID, schemaID)
	resp.TableMetadata[0].TableJson = tableJSON
	return resp
}

func metadataResponseForBuckets(host string, port int32, path TablePath, tableID int64, schemaID int32, bucketIDs ...int32) *flusspb.MetadataResponse {
	if len(bucketIDs) == 0 {
		bucketIDs = []int32{0}
	}
	resp := metadataResponseForSingleBucket(host, port, path, tableID, schemaID)
	resp.TableMetadata[0].BucketMetadata = nil
	for _, bucketID := range bucketIDs {
		resp.TableMetadata[0].BucketMetadata = append(resp.TableMetadata[0].BucketMetadata, &flusspb.PbBucketMetadata{
			BucketId:    proto.Int32(bucketID),
			LeaderId:    proto.Int32(1),
			ReplicaId:   []int32{1},
			LeaderEpoch: proto.Int32(1),
		})
	}
	return resp
}

func TestDialAndAdminFlow(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(
			flusspb.ApiKey_APIVersions,
			flusspb.ApiKey_GetMetadata,
			flusspb.ApiKey_ListDatabases,
			flusspb.ApiKey_DatabaseExists,
			flusspb.ApiKey_CreateTable,
			flusspb.ApiKey_AlterTable,
			flusspb.ApiKey_DropTable,
			flusspb.ApiKey_GetTableInfo,
			flusspb.ApiKey_GetTableSchema,
			flusspb.ApiKey_ListTables,
			flusspb.ApiKey_TableExists,
			flusspb.ApiKey_ListPartitionInfos,
			flusspb.ApiKey_CreatePartition,
			flusspb.ApiKey_DropPartition,
			flusspb.ApiKey_ListAcls,
			flusspb.ApiKey_CreateAcls,
			flusspb.ApiKey_DropAcls,
			flusspb.ApiKey_DescribeClusterConfigs,
			flusspb.ApiKey_AlterClusterConfigs,
			flusspb.ApiKey_AddServerTag,
			flusspb.ApiKey_RemoveServerTag,
			flusspb.ApiKey_Rebalance,
			flusspb.ApiKey_ListRebalanceProgress,
			flusspb.ApiKey_CancelRebalance,
			flusspb.ApiKey_LimitScan,
			flusspb.ApiKey_PrefixLookup,
		)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		resp := metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "events"}, 10, 3)
		resp.TableMetadata[0].TableJson = []byte(`{"name":"events"}`)
		resp.TableMetadata[0].CreatedTime = proto.Int64(1)
		resp.TableMetadata[0].ModifiedTime = proto.Int64(2)
		return mustMarshal(t, resp), nil
	})

	srv.on(flusspb.ApiKey_ListDatabases, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, &flusspb.ListDatabasesResponse{
			DatabaseName: []string{"demo"},
			DatabaseSummary: []*flusspb.PbDatabaseSummary{{
				DatabaseName: proto.String("demo"),
				CreatedTime:  proto.Int64(123),
				TableCount:   proto.Int32(1),
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_DatabaseExists, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.DatabaseExistsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		return mustMarshal(t, &flusspb.DatabaseExistsResponse{
			Exists: proto.Bool(req.GetDatabaseName() == "demo"),
		}), nil
	})

	srv.on(flusspb.ApiKey_GetTableInfo, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, &flusspb.GetTableInfoResponse{
			TableId:      proto.Int64(10),
			SchemaId:     proto.Int32(3),
			TableJson:    []byte(`{"name":"events"}`),
			CreatedTime:  proto.Int64(1),
			ModifiedTime: proto.Int64(2),
		}), nil
	})

	srv.on(flusspb.ApiKey_GetTableSchema, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, &flusspb.GetTableSchemaResponse{
			SchemaId:   proto.Int32(3),
			SchemaJson: []byte(`{"fields":[{"name":"id"}]}`),
		}), nil
	})

	srv.on(flusspb.ApiKey_AlterTable, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.AlterTableRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "events" {
			t.Fatalf("unexpected alter table path: %#v", req.GetTablePath())
		}
		if req.GetIgnoreIfNotExists() {
			t.Fatalf("expected ignore_if_not_exists to be false")
		}
		if len(req.GetConfigChanges()) != 1 || len(req.GetAddColumns()) != 1 || len(req.GetDropColumns()) != 1 || len(req.GetRenameColumns()) != 1 || len(req.GetModifyColumns()) != 1 {
			t.Fatalf("unexpected alter table change counts: %#v", req)
		}
		if req.GetConfigChanges()[0].GetConfigKey() != "client.connect-timeout" || req.GetConfigChanges()[0].GetConfigValue() != "240s" || req.GetConfigChanges()[0].GetOpType() != int32(AlterConfigSet) {
			t.Fatalf("unexpected config change: %#v", req.GetConfigChanges()[0])
		}
		if req.GetAddColumns()[0].GetColumnName() != "c1" || string(req.GetAddColumns()[0].GetDataTypeJson()) != `{"type":"string"}` || req.GetAddColumns()[0].GetColumnPositionType() != int32(ColumnPositionLast) {
			t.Fatalf("unexpected add column change: %#v", req.GetAddColumns()[0])
		}
		if req.GetDropColumns()[0].GetColumnName() != "legacy_col" {
			t.Fatalf("unexpected drop column change: %#v", req.GetDropColumns()[0])
		}
		if req.GetRenameColumns()[0].GetOldColumnName() != "old_name" || req.GetRenameColumns()[0].GetNewColumnName() != "new_name" {
			t.Fatalf("unexpected rename column change: %#v", req.GetRenameColumns()[0])
		}
		if req.GetModifyColumns()[0].GetColumnName() != "name" || string(req.GetModifyColumns()[0].GetDataTypeJson()) != `{"type":"string","nullable":true}` || req.GetModifyColumns()[0].GetColumnPositionType() != int32(ColumnPositionFirst) {
			t.Fatalf("unexpected modify column change: %#v", req.GetModifyColumns()[0])
		}
		return mustMarshal(t, &flusspb.AlterTableResponse{}), nil
	})

	srv.on(flusspb.ApiKey_CreatePartition, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.CreatePartitionRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "events" {
			t.Fatalf("unexpected create partition path: %#v", req.GetTablePath())
		}
		if !req.GetIgnoreIfNotExists() {
			t.Fatalf("expected ignore_if_not_exists to be true")
		}
		kvs := req.GetPartitionSpec().GetPartitionKeyValues()
		if len(kvs) != 1 || kvs[0].GetKey() != "pt" || kvs[0].GetValue() != "2025" {
			t.Fatalf("unexpected create partition spec: %#v", req.GetPartitionSpec())
		}
		return mustMarshal(t, &flusspb.CreatePartitionResponse{}), nil
	})

	srv.on(flusspb.ApiKey_DropPartition, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.DropPartitionRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "events" {
			t.Fatalf("unexpected drop partition path: %#v", req.GetTablePath())
		}
		if req.GetIgnoreIfNotExists() {
			t.Fatalf("expected ignore_if_not_exists to be false")
		}
		kvs := req.GetPartitionSpec().GetPartitionKeyValues()
		if len(kvs) != 1 || kvs[0].GetKey() != "pt" || kvs[0].GetValue() != "2025" {
			t.Fatalf("unexpected drop partition spec: %#v", req.GetPartitionSpec())
		}
		return mustMarshal(t, &flusspb.DropPartitionResponse{}), nil
	})

	srv.on(flusspb.ApiKey_ListPartitionInfos, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ListPartitionInfosRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "events" {
			t.Fatalf("unexpected list partition infos path: %#v", req.GetTablePath())
		}
		resp := &flusspb.ListPartitionInfosResponse{
			PartitionsInfo: []*flusspb.PbPartitionInfo{
				{
					PartitionId: proto.Int64(101),
					PartitionSpec: &flusspb.PbPartitionSpec{
						PartitionKeyValues: []*flusspb.PbKeyValue{{
							Key:   proto.String("pt"),
							Value: proto.String("2025"),
						}},
					},
					RemoteDataDir: proto.String("s3://bucket/partitions/2025"),
				},
			},
		}
		if req.GetPartialPartitionSpec() != nil {
			kvs := req.GetPartialPartitionSpec().GetPartitionKeyValues()
			if len(kvs) != 1 || kvs[0].GetKey() != "pt" || kvs[0].GetValue() != "2025" {
				t.Fatalf("unexpected partial partition spec: %#v", req.GetPartialPartitionSpec())
			}
		}
		return mustMarshal(t, resp), nil
	})

	srv.on(flusspb.ApiKey_ListAcls, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ListAclsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetAclFilter().GetResourceType() != 1 || req.GetAclFilter().GetOperationType() != 3 || req.GetAclFilter().GetPermissionType() != 4 {
			t.Fatalf("unexpected list acls filter: %#v", req.GetAclFilter())
		}
		return mustMarshal(t, &flusspb.ListAclsResponse{
			Acl: []*flusspb.PbAclInfo{{
				ResourceName:   proto.String("events"),
				ResourceType:   proto.Int32(1),
				PrincipalName:  proto.String("alice"),
				PrincipalType:  proto.String("User"),
				Host:           proto.String("*"),
				OperationType:  proto.Int32(3),
				PermissionType: proto.Int32(4),
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_CreateAcls, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.CreateAclsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetAcl()) != 1 || req.GetAcl()[0].GetResourceName() != "events" || req.GetAcl()[0].GetPrincipalName() != "alice" {
			t.Fatalf("unexpected create acls request: %#v", req)
		}
		return mustMarshal(t, &flusspb.CreateAclsResponse{
			AclRes: []*flusspb.PbCreateAclRespInfo{{
				Acl: req.GetAcl()[0],
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_DropAcls, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.DropAclsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetAclFilter()) != 1 || req.GetAclFilter()[0].GetResourceType() != 1 {
			t.Fatalf("unexpected drop acls request: %#v", req)
		}
		return mustMarshal(t, &flusspb.DropAclsResponse{
			FilterResults: []*flusspb.PbDropAclsFilterResult{{
				MatchingAcls: []*flusspb.PbDropAclsMatchingAcl{{
					Acl: &flusspb.PbAclInfo{
						ResourceName:   proto.String("events"),
						ResourceType:   proto.Int32(1),
						PrincipalName:  proto.String("alice"),
						PrincipalType:  proto.String("User"),
						Host:           proto.String("*"),
						OperationType:  proto.Int32(3),
						PermissionType: proto.Int32(4),
					},
				}},
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_DescribeClusterConfigs, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.DescribeClusterConfigsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		return mustMarshal(t, &flusspb.DescribeClusterConfigsResponse{
			Configs: []*flusspb.PbDescribeConfig{{
				ConfigKey:    proto.String("client.connect-timeout"),
				ConfigValue:  proto.String("240s"),
				ConfigSource: proto.String("DYNAMIC_BROKER_CONFIG"),
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_AlterClusterConfigs, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.AlterClusterConfigsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetAlterConfigs()) != 1 || req.GetAlterConfigs()[0].GetConfigKey() != "client.connect-timeout" || req.GetAlterConfigs()[0].GetConfigValue() != "300s" {
			t.Fatalf("unexpected alter cluster configs request: %#v", req)
		}
		return mustMarshal(t, &flusspb.AlterClusterConfigsResponse{}), nil
	})

	srv.on(flusspb.ApiKey_AddServerTag, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.AddServerTagRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetServerIds()) != 2 || req.GetServerIds()[0] != 1 || req.GetServerIds()[1] != 2 || req.GetServerTag() != 9 {
			t.Fatalf("unexpected add server tag request: %#v", req)
		}
		return mustMarshal(t, &flusspb.AddServerTagResponse{}), nil
	})

	srv.on(flusspb.ApiKey_RemoveServerTag, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.RemoveServerTagRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetServerIds()) != 2 || req.GetServerIds()[0] != 1 || req.GetServerIds()[1] != 2 || req.GetServerTag() != 9 {
			t.Fatalf("unexpected remove server tag request: %#v", req)
		}
		return mustMarshal(t, &flusspb.RemoveServerTagResponse{}), nil
	})

	srv.on(flusspb.ApiKey_Rebalance, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.RebalanceRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if len(req.GetGoals()) != 2 || req.GetGoals()[0] != 1 || req.GetGoals()[1] != 2 {
			t.Fatalf("unexpected rebalance request: %#v", req)
		}
		return mustMarshal(t, &flusspb.RebalanceResponse{
			RebalanceId: proto.String("rb-1"),
		}), nil
	})

	srv.on(flusspb.ApiKey_ListRebalanceProgress, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ListRebalanceProgressRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetRebalanceId() != "rb-1" {
			t.Fatalf("unexpected list rebalance progress request: %#v", req)
		}
		return mustMarshal(t, &flusspb.ListRebalanceProgressResponse{
			RebalanceId:     proto.String("rb-1"),
			RebalanceStatus: proto.Int32(2),
			TableProgress: []*flusspb.PbRebalanceProgressForTable{{
				TableId: proto.Int64(10),
				BucketsProgress: []*flusspb.PbRebalanceProgressForBucket{{
					RebalancePlan: &flusspb.PbRebalancePlanForBucket{
						BucketId:         proto.Int32(0),
						OriginalLeader:   proto.Int32(1),
						NewLeader:        proto.Int32(2),
						OriginalReplicas: []int32{1, 2},
						NewReplicas:      []int32{2, 3},
					},
					RebalanceStatus: proto.Int32(1),
				}},
			}},
		}), nil
	})

	srv.on(flusspb.ApiKey_CancelRebalance, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.CancelRebalanceRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetRebalanceId() != "rb-1" {
			t.Fatalf("unexpected cancel rebalance request: %#v", req)
		}
		return mustMarshal(t, &flusspb.CancelRebalanceResponse{}), nil
	})

	srv.on(flusspb.ApiKey_LimitScan, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.LimitScanRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTableId() != 10 || req.GetBucketId() != 0 || req.GetLimit() != 10 {
			t.Fatalf("unexpected limit scan request: %#v", req)
		}
		return mustMarshal(t, &flusspb.LimitScanResponse{
			IsLogTable: proto.Bool(true),
			Records:    []byte("batch-data"),
		}), nil
	})

	srv.on(flusspb.ApiKey_PrefixLookup, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.PrefixLookupRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTableId() != 10 || len(req.GetBucketsReq()) != 1 {
			t.Fatalf("unexpected prefix lookup request: %#v", req)
		}
		bucketReq := req.GetBucketsReq()[0]
		if bucketReq.GetBucketId() != 0 || len(bucketReq.GetKeys()) != 1 {
			t.Fatalf("unexpected prefix lookup bucket request: %#v", bucketReq)
		}
		return mustMarshal(t, &flusspb.PrefixLookupResponse{
			BucketsResp: []*flusspb.PbPrefixLookupRespForBucket{{
				BucketId: proto.Int32(0),
				ValueLists: []*flusspb.PbValueList{{
					Values: [][]byte{[]byte("prefix-row-1"), []byte("prefix-row-2")},
				}},
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	dbs, summaries, err := cli.Admin().ListDatabases(ctx, true)
	if err != nil {
		t.Fatalf("ListDatabases() error = %v", err)
	}
	if len(dbs) != 1 || dbs[0] != "demo" {
		t.Fatalf("unexpected databases: %#v", dbs)
	}
	if len(summaries) != 1 || summaries[0].TableCount != 1 {
		t.Fatalf("unexpected summaries: %#v", summaries)
	}

	exists, err := cli.Admin().DatabaseExists(ctx, "demo")
	if err != nil {
		t.Fatalf("DatabaseExists() error = %v", err)
	}
	if !exists {
		t.Fatal("expected demo database to exist")
	}

	info, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "events"}).Info(ctx)
	if err != nil {
		t.Fatalf("Table.Info() error = %v", err)
	}
	if info.ID != 10 || info.SchemaID != 3 {
		t.Fatalf("unexpected table info: %#v", info)
	}

	schema, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "events"}).Schema(ctx, nil)
	if err != nil {
		t.Fatalf("Table.Schema() error = %v", err)
	}
	if schema.SchemaID != 3 {
		t.Fatalf("unexpected schema: %#v", schema)
	}

	comment := "renamed column"
	firstPos := ColumnPositionFirst
	timeoutValue := "240s"
	if err := cli.Admin().AlterTable(ctx, TablePath{DatabaseName: "demo", TableName: "events"}, []AlterTableChange{
		TableConfigChange{Key: "client.connect-timeout", Value: &timeoutValue, Op: AlterConfigSet},
		AddColumnChange{ColumnName: "c1", DataTypeJSON: []byte(`{"type":"string"}`), ColumnPositionType: ColumnPositionLast},
		DropColumnChange{ColumnName: "legacy_col"},
		RenameColumnChange{OldColumnName: "old_name", NewColumnName: "new_name"},
		ModifyColumnChange{ColumnName: "name", DataTypeJSON: []byte(`{"type":"string","nullable":true}`), Comment: &comment, ColumnPositionType: &firstPos},
	}, false); err != nil {
		t.Fatalf("AlterTable() error = %v", err)
	}

	if err := cli.Admin().CreatePartition(ctx, TablePath{DatabaseName: "demo", TableName: "events"}, PartitionSpec{
		{Key: "pt", Value: "2025"},
	}, true); err != nil {
		t.Fatalf("CreatePartition() error = %v", err)
	}

	partitions, err := cli.Admin().ListPartitionInfosWithSpec(ctx, TablePath{DatabaseName: "demo", TableName: "events"}, PartitionSpec{
		{Key: "pt", Value: "2025"},
	})
	if err != nil {
		t.Fatalf("ListPartitionInfosWithSpec() error = %v", err)
	}
	if len(partitions) != 1 || partitions[0].PartitionID != 101 || partitions[0].RemoteDataDir != "s3://bucket/partitions/2025" {
		t.Fatalf("unexpected filtered partition infos: %#v", partitions)
	}
	if len(partitions[0].PartitionSpec) != 1 || partitions[0].PartitionSpec[0].Key != "pt" || partitions[0].PartitionSpec[0].Value != "2025" {
		t.Fatalf("unexpected filtered partition spec: %#v", partitions[0].PartitionSpec)
	}

	if err := cli.Admin().DropPartition(ctx, TablePath{DatabaseName: "demo", TableName: "events"}, PartitionSpec{
		{Key: "pt", Value: "2025"},
	}, false); err != nil {
		t.Fatalf("DropPartition() error = %v", err)
	}

	acls, err := cli.Admin().ListACLs(ctx, ACLFilter{
		ResourceType:   1,
		OperationType:  3,
		PermissionType: 4,
	})
	if err != nil {
		t.Fatalf("ListACLs() error = %v", err)
	}
	if len(acls) != 1 || acls[0].ResourceName != "events" || acls[0].PrincipalName != "alice" {
		t.Fatalf("unexpected acl bindings: %#v", acls)
	}

	createACLResults, err := cli.Admin().CreateACLs(ctx, []ACLBinding{{
		ResourceName:   "events",
		ResourceType:   1,
		PrincipalName:  "alice",
		PrincipalType:  "User",
		Host:           "*",
		OperationType:  3,
		PermissionType: 4,
	}})
	if err != nil {
		t.Fatalf("CreateACLs() error = %v", err)
	}
	if len(createACLResults) != 1 || createACLResults[0].ACL.ResourceName != "events" {
		t.Fatalf("unexpected create acl results: %#v", createACLResults)
	}

	dropACLResults, err := cli.Admin().DropACLs(ctx, []ACLFilter{{
		ResourceType:   1,
		OperationType:  3,
		PermissionType: 4,
	}})
	if err != nil {
		t.Fatalf("DropACLs() error = %v", err)
	}
	if len(dropACLResults) != 1 || len(dropACLResults[0].MatchingACLs) != 1 || dropACLResults[0].MatchingACLs[0].ACL.PrincipalName != "alice" {
		t.Fatalf("unexpected drop acl results: %#v", dropACLResults)
	}

	clusterConfigs, err := cli.Admin().DescribeClusterConfigs(ctx)
	if err != nil {
		t.Fatalf("DescribeClusterConfigs() error = %v", err)
	}
	if len(clusterConfigs) != 1 || clusterConfigs[0].Key != "client.connect-timeout" || clusterConfigs[0].Value == nil || *clusterConfigs[0].Value != "240s" {
		t.Fatalf("unexpected cluster configs: %#v", clusterConfigs)
	}

	clusterTimeout := "300s"
	if err := cli.Admin().AlterClusterConfigs(ctx, []TableConfigChange{{
		Key:   "client.connect-timeout",
		Value: &clusterTimeout,
		Op:    AlterConfigSet,
	}}); err != nil {
		t.Fatalf("AlterClusterConfigs() error = %v", err)
	}

	if err := cli.Admin().AddServerTag(ctx, []int32{1, 2}, ServerTag(9)); err != nil {
		t.Fatalf("AddServerTag() error = %v", err)
	}
	if err := cli.Admin().RemoveServerTag(ctx, []int32{1, 2}, ServerTag(9)); err != nil {
		t.Fatalf("RemoveServerTag() error = %v", err)
	}

	rebalanceID, err := cli.Admin().Rebalance(ctx, []RebalanceGoal{1, 2})
	if err != nil {
		t.Fatalf("Rebalance() error = %v", err)
	}
	if rebalanceID != "rb-1" {
		t.Fatalf("unexpected rebalance id: %q", rebalanceID)
	}

	progress, err := cli.Admin().ListRebalanceProgress(ctx, &rebalanceID)
	if err != nil {
		t.Fatalf("ListRebalanceProgress() error = %v", err)
	}
	if progress == nil || progress.RebalanceID != "rb-1" || progress.Status == nil || *progress.Status != 2 {
		t.Fatalf("unexpected rebalance progress: %#v", progress)
	}
	if len(progress.Tables) != 1 || len(progress.Tables[0].Buckets) != 1 || progress.Tables[0].Buckets[0].Plan.BucketID != 0 {
		t.Fatalf("unexpected rebalance table progress: %#v", progress.Tables)
	}

	if err := cli.Admin().CancelRebalance(ctx, &rebalanceID); err != nil {
		t.Fatalf("CancelRebalance() error = %v", err)
	}

	limitResult, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "events"}).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		t.Fatalf("LimitScan() error = %v", err)
	}
	if !limitResult.IsLogTable || string(limitResult.Records) != "batch-data" {
		t.Fatalf("unexpected limit result: %#v", limitResult)
	}

	prefixResult, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "events"}).PrefixLookup(ctx, []LookupBucketRequest{{
		BucketID: 0,
		Keys:     [][]byte{[]byte("prefix-key")},
	}})
	if err != nil {
		t.Fatalf("PrefixLookup() error = %v", err)
	}
	if len(prefixResult) != 1 || len(prefixResult[0].Values) != 1 || len(prefixResult[0].Values[0]) != 2 {
		t.Fatalf("unexpected prefix result: %#v", prefixResult)
	}
	if string(prefixResult[0].Values[0][0]) != "prefix-row-1" || string(prefixResult[0].Values[0][1]) != "prefix-row-2" {
		t.Fatalf("unexpected prefix payloads: %#v", prefixResult)
	}
}

func TestKVScannerLifecycle(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ScanKV)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "kv"}, 11, 1)), nil
	})

	callCount := 0
	srv.on(flusspb.ApiKey_ScanKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ScanKvRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		callCount++
		switch callCount {
		case 1:
			if req.GetBucketScanReq() == nil || req.GetBucketScanReq().GetTableId() != 11 {
				t.Fatalf("unexpected initial scan request: %#v", req)
			}
			return mustMarshal(t, &flusspb.ScanKvResponse{
				ScannerId:      []byte("scanner-1"),
				HasMoreResults: proto.Bool(true),
				Records:        []byte("first-batch"),
				LogOffset:      proto.Int64(99),
			}), nil
		default:
			if string(req.GetScannerId()) != "scanner-1" {
				t.Fatalf("expected follow-up scan to reuse scanner id, got %#v", req)
			}
			return mustMarshal(t, &flusspb.ScanKvResponse{
				ScannerId:      []byte("scanner-1"),
				HasMoreResults: proto.Bool(false),
				Records:        []byte("second-batch"),
			}), nil
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	scanner := cli.Table(TablePath{DatabaseName: "demo", TableName: "kv"}).NewKVScanner(nil, 0, nil, 1024)
	first, err := scanner.Next(ctx)
	if err != nil {
		t.Fatalf("scanner.Next() first error = %v", err)
	}
	if string(first.ScannerID) != "scanner-1" || string(first.Records) != "first-batch" || !first.HasMoreResults {
		t.Fatalf("unexpected first result: %#v", first)
	}

	second, err := scanner.Next(ctx)
	if err != nil {
		t.Fatalf("scanner.Next() second error = %v", err)
	}
	if second.HasMoreResults || string(second.Records) != "second-batch" {
		t.Fatalf("unexpected second result: %#v", second)
	}

	if err := scanner.Close(ctx); err != nil {
		t.Fatalf("scanner.Close() error = %v", err)
	}
}

func TestDecodeIndexedLimitScanRows(t *testing.T) {
	logSchema := NewSchema(Int64Type(), StringType())
	logRow, err := NewRow(logSchema, int64(7), "event")
	if err != nil {
		t.Fatalf("NewRow(log) error = %v", err)
	}
	logPayload, err := rowcodec.EncodeLogRecordBatch(logSchema, logRow.Values, rowcodec.LogBatchOptions{SchemaID: 1, Indexed: true})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}
	logRows, err := DecodeIndexedLimitScanRows(logSchema, LimitScanResult{IsLogTable: true, Records: logPayload})
	if err != nil {
		t.Fatalf("DecodeIndexedLimitScanRows(log) error = %v", err)
	}
	if len(logRows) != 1 || logRows[0][0] != int64(7) || logRows[0][1] != "event" {
		t.Fatalf("unexpected log rows: %#v", logRows)
	}

	kvSchema := NewSchema(Int64Type(), StringType(), StringType())
	kvRow, err := NewRow(kvSchema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		t.Fatalf("NewRow(kv) error = %v", err)
	}
	kvPayload, err := rowcodec.EncodeKvRecordBatch(kvSchema, kvRow.Values, rowcodec.KvBatchOptions{SchemaID: 1, Indexed: true, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch() error = %v", err)
	}
	_, recordPayload, err := decodeKvBatchForTest(kvPayload)
	if err != nil {
		t.Fatalf("decodeKvBatchForTest() error = %v", err)
	}
	keyLen, n := binary.Uvarint(recordPayload[4:])
	if n <= 0 {
		t.Fatalf("invalid key length varint")
	}
	rowPayload := recordPayload[4+n+int(keyLen):]
	valueRecord := make([]byte, 0, 4+2+len(rowPayload))
	valueRecord = binary.LittleEndian.AppendUint32(valueRecord, uint32(2+len(rowPayload)))
	valueRecord = binary.LittleEndian.AppendUint16(valueRecord, 1)
	valueRecord = append(valueRecord, rowPayload...)
	valueBatch := make([]byte, 0, 9+len(valueRecord))
	valueBatch = binary.LittleEndian.AppendUint32(valueBatch, uint32(5+len(valueRecord)))
	valueBatch = append(valueBatch, 0)
	valueBatch = binary.LittleEndian.AppendUint32(valueBatch, 1)
	valueBatch = append(valueBatch, valueRecord...)

	kvRows, err := DecodeIndexedLimitScanRows(kvSchema, LimitScanResult{IsLogTable: false, Records: valueBatch})
	if err != nil {
		t.Fatalf("DecodeIndexedLimitScanRows(kv) error = %v", err)
	}
	if len(kvRows) != 1 || kvRows[0][0] != int64(42) || kvRows[0][1] != "Ada Lovelace" || kvRows[0][2] != "gold" {
		t.Fatalf("unexpected kv rows: %#v", kvRows)
	}
}

func TestDecodeCompactedLimitScanRows(t *testing.T) {
	kvSchema := NewSchema(Int64Type(), StringType(), StringType())
	kvRow, err := NewRow(kvSchema, int64(42), "Ada Lovelace", "diamond")
	if err != nil {
		t.Fatalf("NewRow(kv) error = %v", err)
	}
	kvPayload, err := rowcodec.EncodeKvRecordBatch(kvSchema, kvRow.Values, rowcodec.KvBatchOptions{SchemaID: 1, Indexed: false, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch() error = %v", err)
	}
	_, recordPayload, err := decodeKvBatchForTest(kvPayload)
	if err != nil {
		t.Fatalf("decodeKvBatchForTest() error = %v", err)
	}
	keyLen, n := binary.Uvarint(recordPayload[4:])
	if n <= 0 {
		t.Fatalf("invalid key length varint")
	}
	rowPayload := recordPayload[4+n+int(keyLen):]
	valueRecord := make([]byte, 0, 4+2+len(rowPayload))
	valueRecord = binary.LittleEndian.AppendUint32(valueRecord, uint32(2+len(rowPayload)))
	valueRecord = binary.LittleEndian.AppendUint16(valueRecord, 1)
	valueRecord = append(valueRecord, rowPayload...)
	valueBatch := make([]byte, 0, 9+len(valueRecord))
	valueBatch = binary.LittleEndian.AppendUint32(valueBatch, uint32(5+len(valueRecord)))
	valueBatch = append(valueBatch, 0)
	valueBatch = binary.LittleEndian.AppendUint32(valueBatch, 1)
	valueBatch = append(valueBatch, valueRecord...)

	kvRows, err := DecodeLimitScanRows(kvSchema, LimitScanResult{IsLogTable: false, Records: valueBatch}, false)
	if err != nil {
		t.Fatalf("DecodeLimitScanRows(compacted kv) error = %v", err)
	}
	if len(kvRows) != 1 || kvRows[0][0] != int64(42) || kvRows[0][1] != "Ada Lovelace" || kvRows[0][2] != "diamond" {
		t.Fatalf("unexpected compacted kv rows: %#v", kvRows)
	}
}

func TestAdminSnapshotMetadata(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(
			flusspb.ApiKey_APIVersions,
			flusspb.ApiKey_GetMetadata,
			flusspb.ApiKey_GetLatestKvSnapshots,
			flusspb.ApiKey_GetKvSnapshotMetadata,
			flusspb.ApiKey_GetLakeSnapshot,
		)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, &flusspb.MetadataResponse{}), nil
	})

	srv.on(flusspb.ApiKey_GetLatestKvSnapshots, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.GetLatestKvSnapshotsRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "customers" {
			t.Fatalf("unexpected table path: %#v", req.GetTablePath())
		}
		return mustMarshal(t, &flusspb.GetLatestKvSnapshotsResponse{
			TableId: proto.Int64(51),
			LatestSnapshots: []*flusspb.PbKvSnapshot{
				{BucketId: proto.Int32(0), SnapshotId: proto.Int64(7), LogOffset: proto.Int64(101)},
				{BucketId: proto.Int32(1)},
			},
		}), nil
	})

	srv.on(flusspb.ApiKey_GetKvSnapshotMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.GetKvSnapshotMetadataRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTableId() != 51 || req.GetBucketId() != 0 || req.GetSnapshotId() != 7 {
			t.Fatalf("unexpected snapshot metadata request: %#v", req)
		}
		return mustMarshal(t, &flusspb.GetKvSnapshotMetadataResponse{
			LogOffset: proto.Int64(101),
			SnapshotFiles: []*flusspb.PbRemotePathAndLocalFile{
				{RemotePath: proto.String("s3://bucket/snapshots/0001.sst"), LocalFileName: proto.String("0001.sst")},
				{RemotePath: proto.String("s3://bucket/snapshots/MANIFEST"), LocalFileName: proto.String("MANIFEST")},
			},
		}), nil
	})

	srv.on(flusspb.ApiKey_GetLakeSnapshot, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.GetLakeSnapshotRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTablePath().GetDatabaseName() != "demo" || req.GetTablePath().GetTableName() != "customers" {
			t.Fatalf("unexpected lake snapshot table path: %#v", req.GetTablePath())
		}
		return mustMarshal(t, &flusspb.GetLakeSnapshotResponse{
			TableId:    proto.Int64(51),
			SnapshotId: proto.Int64(12),
			BucketSnapshots: []*flusspb.PbLakeSnapshotForBucket{
				{BucketId: proto.Int32(0), LogOffset: proto.Int64(111)},
				{BucketId: proto.Int32(1), LogOffset: proto.Int64(222)},
			},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	snapshots, err := cli.Admin().GetLatestKvSnapshots(ctx, TablePath{DatabaseName: "demo", TableName: "customers"}, nil)
	if err != nil {
		t.Fatalf("GetLatestKvSnapshots() error = %v", err)
	}
	if snapshots.TableID != 51 {
		t.Fatalf("unexpected table id: %d", snapshots.TableID)
	}
	if got := snapshots.SnapshotIDs[0]; got == nil || *got != 7 {
		t.Fatalf("unexpected snapshot id for bucket 0: %#v", got)
	}
	if got := snapshots.LogOffsets[0]; got == nil || *got != 101 {
		t.Fatalf("unexpected log offset for bucket 0: %#v", got)
	}
	if got := snapshots.SnapshotIDs[1]; got != nil {
		t.Fatalf("unexpected snapshot id for bucket 1: %#v", got)
	}

	metadata, err := cli.Admin().GetKvSnapshotMetadata(ctx, 51, nil, 0, 7)
	if err != nil {
		t.Fatalf("GetKvSnapshotMetadata() error = %v", err)
	}
	if metadata.LogOffset != 101 || len(metadata.SnapshotFiles) != 2 {
		t.Fatalf("unexpected snapshot metadata: %#v", metadata)
	}
	if metadata.SnapshotFiles[0].RemotePath != "s3://bucket/snapshots/0001.sst" {
		t.Fatalf("unexpected first snapshot file: %#v", metadata.SnapshotFiles[0])
	}

	lakeSnapshot, err := cli.Admin().GetLatestLakeSnapshot(ctx, TablePath{DatabaseName: "demo", TableName: "customers"})
	if err != nil {
		t.Fatalf("GetLatestLakeSnapshot() error = %v", err)
	}
	if lakeSnapshot.TableID != 51 || lakeSnapshot.SnapshotID != 12 || len(lakeSnapshot.Buckets) != 2 {
		t.Fatalf("unexpected lake snapshot: %#v", lakeSnapshot)
	}
	if lakeSnapshot.Buckets[0].BucketID != 0 || lakeSnapshot.Buckets[0].LogOffset != 111 {
		t.Fatalf("unexpected first lake snapshot bucket: %#v", lakeSnapshot.Buckets[0])
	}
}

func TestSnapshotScanRows(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(
			flusspb.ApiKey_APIVersions,
			flusspb.ApiKey_GetMetadata,
			flusspb.ApiKey_GetLatestKvSnapshots,
			flusspb.ApiKey_GetKvSnapshotMetadata,
		)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucketWithJSON(host, int32(port), TablePath{DatabaseName: "demo", TableName: "customers"}, 51, 7, []byte(`{
			"schema":{
				"columns":[
					{"name":"customer_id","type":"BIGINT"},
					{"name":"customer_name","type":"STRING"},
					{"name":"customer_tier","type":"STRING"}],
				"primary_key":["customer_id"]
			},
			"properties":{"table.kv.format":"compacted"}
		}`))), nil
	})

	srv.on(flusspb.ApiKey_GetLatestKvSnapshots, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, &flusspb.GetLatestKvSnapshotsResponse{
			TableId: proto.Int64(51),
			LatestSnapshots: []*flusspb.PbKvSnapshot{
				{BucketId: proto.Int32(0), SnapshotId: proto.Int64(7), LogOffset: proto.Int64(101)},
			},
		}), nil
	})

	srv.on(flusspb.ApiKey_GetKvSnapshotMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.GetKvSnapshotMetadataRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetTableId() != 51 || req.GetBucketId() != 0 || req.GetSnapshotId() != 7 {
			t.Fatalf("unexpected snapshot metadata request: %#v", req)
		}
		return mustMarshal(t, &flusspb.GetKvSnapshotMetadataResponse{
			LogOffset: proto.Int64(101),
			SnapshotFiles: []*flusspb.PbRemotePathAndLocalFile{
				{RemotePath: proto.String("s3://fluss/snapshots/0001.sst"), LocalFileName: proto.String("0001.sst")},
			},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	localDir := filepath.Join(t.TempDir(), "db")
	db, err := pebble.Open(localDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	schema := NewSchema(Int64Type(), StringType(), StringType())
	rowPayload, err := rowcodec.Row{Schema: rowcodec.Schema(schema), Values: []any{int64(42), "Ada Lovelace", "diamond"}}.EncodeCompacted()
	if err != nil {
		t.Fatalf("EncodeCompacted() error = %v", err)
	}
	value := make([]byte, 2, 2+len(rowPayload))
	binary.LittleEndian.PutUint16(value[:2], uint16(7))
	value = append(value, rowPayload...)
	if err := db.Set([]byte("k1"), value, pebble.Sync); err != nil {
		t.Fatalf("db.Set() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	rows, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "customers"}).snapshotScanRows(
		ctx,
		schema,
		SnapshotScanOptions{BucketID: 0},
		func(context.Context, []snapshot.RemoteFile) (string, error) { return localDir, nil },
	)
	if err != nil {
		t.Fatalf("snapshotScanRows() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count = %d, want 1", len(rows))
	}
	if rows[0][0] != int64(42) || rows[0][1] != "Ada Lovelace" || rows[0][2] != "diamond" {
		t.Fatalf("unexpected row: %#v", rows[0])
	}
	_ = os.RemoveAll(localDir)
}

func TestFetchLogWithProjection(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_FetchLog)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "logs"}, 31, 2)), nil
	})

	srv.on(flusspb.ApiKey_FetchLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.FetchLogRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetFollowerServerId() != -1 || req.GetMaxBytes() != 4096 {
			t.Fatalf("unexpected fetch request header: %#v", req)
		}
		if len(req.GetTablesReq()) != 1 {
			t.Fatalf("unexpected tables request: %#v", req)
		}
		tableReq := req.GetTablesReq()[0]
		if !tableReq.GetProjectionPushdownEnabled() {
			t.Fatalf("expected projection pushdown enabled: %#v", tableReq)
		}
		if len(tableReq.GetProjectedFields()) != 2 || tableReq.GetProjectedFields()[0] != 0 || tableReq.GetProjectedFields()[1] != 3 {
			t.Fatalf("unexpected projected fields: %#v", tableReq.GetProjectedFields())
		}
		if len(tableReq.GetBucketsReq()) != 1 || tableReq.GetBucketsReq()[0].GetFetchOffset() != 10 {
			t.Fatalf("unexpected bucket request: %#v", tableReq.GetBucketsReq())
		}
		return mustMarshal(t, &flusspb.FetchLogResponse{
			TablesResp: []*flusspb.PbFetchLogRespForTable{{
				TableId: proto.Int64(31),
				BucketsResp: []*flusspb.PbFetchLogRespForBucket{{
					BucketId:       proto.Int32(0),
					HighWatermark:  proto.Int64(15),
					LogStartOffset: proto.Int64(0),
					Records:        []byte("projected-log-batch"),
				}},
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	got, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "logs"}).FetchLogWithOptions(ctx, -1, 4096, nil, nil, []FetchBucketRequest{{
		BucketID:      0,
		FetchOffset:   10,
		MaxFetchBytes: 1024,
	}}, FetchLogOptions{ProjectedFields: []int32{0, 3}})
	if err != nil {
		t.Fatalf("FetchLogWithOptions() error = %v", err)
	}
	if len(got) != 1 || string(got[0].Records) != "projected-log-batch" || got[0].HighWatermark != 15 {
		t.Fatalf("unexpected fetch results: %#v", got)
	}
}

func TestAppendArrowRowsAndDecode(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ProduceLog)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "arrow_logs"}, 41, 9)), nil
	})

	srv.on(flusspb.ApiKey_ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ProduceLogRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		records := req.GetBucketsReq()[0].GetRecords()
		schema := NewSchema(Int64Type(), Int32Type(), StringType())
		decoded, err := DecodeArrowLogBatchRows(schema, records)
		if err != nil {
			t.Fatalf("DecodeArrowLogBatchRows() error = %v", err)
		}
		if len(decoded) != 2 || decoded[0][0] != int64(1) || decoded[1][2] != "packed" {
			t.Fatalf("unexpected decoded arrow rows: %#v", decoded)
		}
		return mustMarshal(t, &flusspb.ProduceLogResponse{
			BucketsResp: []*flusspb.PbProduceLogRespForBucket{{
				BucketId:   proto.Int32(0),
				BaseOffset: proto.Int64(88),
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	schema := NewSchema(Int64Type(), Int32Type(), StringType())
	row1, err := NewRow(schema, int64(1), int32(101), "created")
	if err != nil {
		t.Fatalf("NewRow(row1): %v", err)
	}
	row2, err := NewRow(schema, int64(2), int32(102), "packed")
	if err != nil {
		t.Fatalf("NewRow(row2): %v", err)
	}
	got, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "arrow_logs"}).AppendArrowRows(ctx, 0, []Row{row1, row2})
	if err != nil {
		t.Fatalf("AppendArrowRows() error = %v", err)
	}
	if len(got) != 1 || got[0].BaseOffset != 88 {
		t.Fatalf("unexpected append results: %#v", got)
	}
}

func TestAppendArrowRowsUsesDefaultZstdCompression(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ProduceLog)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		resp := metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "arrow_logs"}, 42, 9)
		resp.TableMetadata[0].TableJson = []byte(`{"schema":{"columns":[{"name":"order_id"},{"name":"customer_id"},{"name":"status"}],"primary_key":[]},"partition_key":[],"bucket_key":[],"properties":{"table.log.arrow.compression.type":"ZSTD","table.log.arrow.compression.zstd.level":"3"}}`)
		return mustMarshal(t, resp), nil
	})

	srv.on(flusspb.ApiKey_ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.ProduceLogRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		records := req.GetBucketsReq()[0].GetRecords()
		schema := NewSchema(Int64Type(), Int32Type(), StringType())
		decoded, err := DecodeArrowLogBatchRows(schema, records)
		if err != nil {
			t.Fatalf("DecodeArrowLogBatchRows() error = %v", err)
		}
		if len(decoded) != 2 || decoded[0][0] != int64(1) || decoded[1][2] != "packed" {
			t.Fatalf("unexpected decoded zstd arrow rows: %#v", decoded)
		}
		return mustMarshal(t, &flusspb.ProduceLogResponse{
			BucketsResp: []*flusspb.PbProduceLogRespForBucket{{
				BucketId:   proto.Int32(0),
				BaseOffset: proto.Int64(91),
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	schema := NewSchema(Int64Type(), Int32Type(), StringType())
	row1, _ := NewRow(schema, int64(1), int32(101), "created")
	row2, _ := NewRow(schema, int64(2), int32(102), "packed")
	got, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "arrow_logs"}).AppendArrowRows(ctx, 0, []Row{row1, row2})
	if err != nil {
		t.Fatalf("AppendArrowRows() error = %v", err)
	}
	if len(got) != 1 || got[0].BaseOffset != 91 {
		t.Fatalf("unexpected append results: %#v", got)
	}
}

func TestDecodeArrowLogBatchRows(t *testing.T) {
	schema := NewSchema(Int64Type(), Int32Type(), StringType())
	payload, err := arrowcodec.EncodeLogRecordBatch(schema, [][]any{
		{int64(1), int32(101), "created"},
		{int64(2), int32(102), "packed"},
	}, arrowcodec.LogBatchOptions{SchemaID: 3, AppendOnly: true})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}
	got, err := DecodeArrowLogBatchRows(schema, payload)
	if err != nil {
		t.Fatalf("DecodeArrowLogBatchRows() error = %v", err)
	}
	if len(got) != 2 || got[0][0] != int64(1) || got[1][2] != "packed" {
		t.Fatalf("unexpected decoded rows: %#v", got)
	}
}

func TestDecodeProjectedArrowLogBatchRowsSkipsCRCValidation(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType())
	payload, err := arrowcodec.EncodeLogRecordBatch(schema, [][]any{
		{int64(1), "created"},
		{int64(2), "packed"},
	}, arrowcodec.LogBatchOptions{SchemaID: 3, AppendOnly: true})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}
	payload[21] ^= 0xFF

	if _, err := DecodeArrowLogBatchRows(schema, payload); err == nil {
		t.Fatal("DecodeArrowLogBatchRows() error = nil, want CRC validation failure")
	}
	got, err := DecodeProjectedArrowLogBatchRows(schema, payload)
	if err != nil {
		t.Fatalf("DecodeProjectedArrowLogBatchRows() error = %v", err)
	}
	if len(got) != 2 || got[0][0] != int64(1) || got[1][1] != "packed" {
		t.Fatalf("unexpected projected decoded rows: %#v", got)
	}
}

func decodeKvBatchForTest(payload []byte) (int32, []byte, error) {
	if len(payload) < 28 {
		return 0, nil, fmt.Errorf("kv batch too short")
	}
	return int32(binary.LittleEndian.Uint16(payload[9:11])), payload[28:], nil
}

func TestAppendWriterLifecycleAndDefaults(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ProduceLog)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForBuckets(host, int32(port), TablePath{DatabaseName: "demo", TableName: "logs"}, 21, 2, 0, 1)), nil
	})

	var produceCalls int
	srv.on(flusspb.ApiKey_ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		produceCalls++
		req := &flusspb.ProduceLogRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetAcks() != -1 {
			t.Fatalf("expected default acks -1, got %d", req.GetAcks())
		}
		if req.GetTimeoutMs() != 15000 {
			t.Fatalf("expected default timeout 15000, got %d", req.GetTimeoutMs())
		}
		return mustMarshal(t, &flusspb.ProduceLogResponse{
			BucketsResp: []*flusspb.PbProduceLogRespForBucket{{
				BucketId:   proto.Int32(0),
				BaseOffset: proto.Int64(42),
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	writer := cli.Table(TablePath{DatabaseName: "demo", TableName: "logs"}).NewAppendWriter(AppendOptions{})
	results, err := writer.Write(ctx, []BucketRecordBatch{{
		BucketID: 0,
		Records:  []byte("log-batch"),
	}})
	if err != nil {
		t.Fatalf("writer.Write() error = %v", err)
	}
	if len(results) != 1 || results[0].BaseOffset != 42 {
		t.Fatalf("unexpected produce results: %#v", results)
	}
	if produceCalls != 1 {
		t.Fatalf("expected 1 produce call, got %d", produceCalls)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	if _, err := writer.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("x")}}); err == nil || err != ErrClosed {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestUpsertWriterLifecycleAndOptions(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_PutKV)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "kv"}, 31, 5)), nil
	})

	aggMode := int32(2)
	var putCalls int
	srv.on(flusspb.ApiKey_PutKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		putCalls++
		req := &flusspb.PutKvRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if req.GetAcks() != 1 {
			t.Fatalf("expected configured acks 1, got %d", req.GetAcks())
		}
		if req.GetTimeoutMs() != 9000 {
			t.Fatalf("expected configured timeout 9000, got %d", req.GetTimeoutMs())
		}
		targetColumns := req.GetTargetColumns()
		if len(targetColumns) != 2 || targetColumns[0] != 1 || targetColumns[1] != 3 {
			t.Fatalf("unexpected target_columns: %#v", targetColumns)
		}
		if req.GetAggMode() != aggMode {
			t.Fatalf("expected agg_mode %d, got %d", aggMode, req.GetAggMode())
		}
		return mustMarshal(t, &flusspb.PutKvResponse{
			BucketsResp: []*flusspb.PbPutKvRespForBucket{{
				BucketId:     proto.Int32(0),
				LogEndOffset: proto.Int64(77),
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	writer := cli.Table(TablePath{DatabaseName: "demo", TableName: "kv"}).NewUpsertWriter(UpsertOptions{
		Acks:          1,
		TimeoutMs:     9000,
		TargetColumns: []int32{1, 3},
		AggMode:       &aggMode,
	})
	results, err := writer.Write(ctx, []BucketRecordBatch{{
		BucketID: 0,
		Records:  []byte("kv-batch"),
	}})
	if err != nil {
		t.Fatalf("writer.Write() error = %v", err)
	}
	if len(results) != 1 || results[0].LogEndOffset != 77 {
		t.Fatalf("unexpected put results: %#v", results)
	}
	if putCalls != 1 {
		t.Fatalf("expected 1 put call, got %d", putCalls)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	if _, err := writer.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("x")}}); err == nil || err != ErrClosed {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestAppendWriterBufferedFlushAndCloseWithContext(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ProduceLog)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForBuckets(host, int32(port), TablePath{DatabaseName: "demo", TableName: "logs"}, 21, 2, 0, 1)), nil
	})

	var produceCalls int
	var lastBucketCount int
	srv.on(flusspb.ApiKey_ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		produceCalls++
		req := &flusspb.ProduceLogRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		lastBucketCount = len(req.GetBucketsReq())
		return mustMarshal(t, &flusspb.ProduceLogResponse{
			BucketsResp: []*flusspb.PbProduceLogRespForBucket{
				{BucketId: proto.Int32(0), BaseOffset: proto.Int64(10)},
				{BucketId: proto.Int32(1), BaseOffset: proto.Int64(11)},
			},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	writer := cli.Table(TablePath{DatabaseName: "demo", TableName: "logs"}).NewAppendWriter(AppendOptions{
		MaxBufferedBatches: 4,
	})
	results, err := writer.Write(ctx, []BucketRecordBatch{
		{BucketID: 0, Records: []byte("a")},
		{BucketID: 1, Records: []byte("b")},
	})
	if err != nil {
		t.Fatalf("writer.Write() error = %v", err)
	}
	if results != nil {
		t.Fatalf("expected nil results for buffered write, got %#v", results)
	}
	if writer.BufferedLen() != 2 {
		t.Fatalf("expected 2 buffered batches, got %d", writer.BufferedLen())
	}
	if produceCalls != 0 {
		t.Fatalf("expected no produce before flush, got %d calls", produceCalls)
	}

	flushed, err := writer.Flush(ctx)
	if err != nil {
		t.Fatalf("writer.Flush() error = %v", err)
	}
	if len(flushed) != 2 {
		t.Fatalf("expected 2 flush results, got %#v", flushed)
	}
	if produceCalls != 1 || lastBucketCount != 2 {
		t.Fatalf("unexpected flush call stats: calls=%d buckets=%d", produceCalls, lastBucketCount)
	}
	if writer.BufferedLen() != 0 {
		t.Fatalf("expected empty buffer after flush, got %d", writer.BufferedLen())
	}

	if _, err := writer.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("c")}}); err != nil {
		t.Fatalf("writer.Write() second buffer error = %v", err)
	}
	if err := writer.CloseWithContext(ctx); err != nil {
		t.Fatalf("writer.CloseWithContext() error = %v", err)
	}
	if produceCalls != 2 {
		t.Fatalf("expected close flush to trigger second produce call, got %d", produceCalls)
	}
	if _, err := writer.Flush(ctx); err == nil || err != ErrClosed {
		t.Fatalf("expected ErrClosed from Flush after close, got %v", err)
	}
}

func TestAppendWriterBufferLimit(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_ProduceLog)), nil
	})
	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "logs"}, 21, 2)), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	bufferedWriter := cli.Table(TablePath{DatabaseName: "demo", TableName: "logs"}).NewAppendWriter(AppendOptions{
		MaxBufferedBatches: 2,
	})
	if _, err := bufferedWriter.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("a")}}); err != nil {
		t.Fatalf("bufferedWriter first write error = %v", err)
	}
	if _, err := bufferedWriter.Write(ctx, []BucketRecordBatch{
		{BucketID: 1, Records: []byte("b")},
		{BucketID: 2, Records: []byte("c")},
	}); err == nil || err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

func TestUpsertWriterBufferedFlushAndCloseWithContext(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_PutKV)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForBuckets(host, int32(port), TablePath{DatabaseName: "demo", TableName: "kv"}, 31, 5, 0, 1)), nil
	})

	var putCalls int
	var lastBucketCount int
	srv.on(flusspb.ApiKey_PutKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		putCalls++
		req := &flusspb.PutKvRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		lastBucketCount = len(req.GetBucketsReq())
		return mustMarshal(t, &flusspb.PutKvResponse{
			BucketsResp: []*flusspb.PbPutKvRespForBucket{
				{BucketId: proto.Int32(0), LogEndOffset: proto.Int64(20)},
				{BucketId: proto.Int32(1), LogEndOffset: proto.Int64(21)},
			},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	writer := cli.Table(TablePath{DatabaseName: "demo", TableName: "kv"}).NewUpsertWriter(UpsertOptions{
		MaxBufferedBatches: 4,
	})
	results, err := writer.Write(ctx, []BucketRecordBatch{
		{BucketID: 0, Records: []byte("a")},
		{BucketID: 1, Records: []byte("b")},
	})
	if err != nil {
		t.Fatalf("writer.Write() error = %v", err)
	}
	if results != nil {
		t.Fatalf("expected nil results for buffered write, got %#v", results)
	}
	if writer.BufferedLen() != 2 {
		t.Fatalf("expected 2 buffered batches, got %d", writer.BufferedLen())
	}
	if putCalls != 0 {
		t.Fatalf("expected no put before flush, got %d calls", putCalls)
	}

	flushed, err := writer.Flush(ctx)
	if err != nil {
		t.Fatalf("writer.Flush() error = %v", err)
	}
	if len(flushed) != 2 {
		t.Fatalf("expected 2 flush results, got %#v", flushed)
	}
	if putCalls != 1 || lastBucketCount != 2 {
		t.Fatalf("unexpected flush call stats: calls=%d buckets=%d", putCalls, lastBucketCount)
	}

	if _, err := writer.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("c")}}); err != nil {
		t.Fatalf("writer.Write() second buffer error = %v", err)
	}
	if err := writer.CloseWithContext(ctx); err != nil {
		t.Fatalf("writer.CloseWithContext() error = %v", err)
	}
	if putCalls != 2 {
		t.Fatalf("expected close flush to trigger second put call, got %d", putCalls)
	}
	if _, err := writer.Flush(ctx); err == nil || err != ErrClosed {
		t.Fatalf("expected ErrClosed from Flush after close, got %v", err)
	}
}

func TestPartialUpdateIndexedRowUsesTargetColumnsAndNullablePayload(t *testing.T) {
	srv := newMockFlussServer(t)
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	srv.on(flusspb.ApiKey_APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, apiVersionsResponse(flusspb.ApiKey_APIVersions, flusspb.ApiKey_GetMetadata, flusspb.ApiKey_PutKV)), nil
	})

	srv.on(flusspb.ApiKey_GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMarshal(t, metadataResponseForSingleBucketWithJSON(host, int32(port), TablePath{DatabaseName: "demo", TableName: "customers"}, 51, 7, []byte(`{
			"table_id":51,
			"table_path":{"database_name":"demo","table_name":"customers"},
			"schema_id":7,
			"properties":{"table.kv.format":"indexed","table.kv.format-version":"2"},
			"primary_key":["customer_id"],
			"columns":[
				{"name":"customer_id","type":"BIGINT"},
				{"name":"customer_name","type":"STRING"},
				{"name":"customer_tier","type":"STRING"}
			]
		}`))), nil
	})

	srv.on(flusspb.ApiKey_PutKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req := &flusspb.PutKvRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		if got := req.GetTargetColumns(); len(got) != 2 || got[0] != 0 || got[1] != 2 {
			t.Fatalf("unexpected target columns: %#v", got)
		}
		if len(req.GetBucketsReq()) != 1 {
			t.Fatalf("unexpected bucket req count: %d", len(req.GetBucketsReq()))
		}
		_, recordPayload, err := decodeKvBatchForTest(req.GetBucketsReq()[0].GetRecords())
		if err != nil {
			t.Fatalf("decodeKvBatchForTest() error = %v", err)
		}
		keyLen, n := binary.Uvarint(recordPayload[4:])
		if n <= 0 {
			t.Fatalf("invalid key varint")
		}
		rowPayload := recordPayload[4+n+int(keyLen):]
		schema := rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.StringType(), rowcodec.StringType())
		got, err := rowcodec.DecodeIndexed(schema, rowPayload)
		if err != nil {
			t.Fatalf("DecodeIndexed() error = %v", err)
		}
		want := []any{int64(42), nil, "diamond"}
		for i := range want {
			if !testValueEqual(got[i], want[i]) {
				t.Fatalf("row[%d] = %#v, want %#v", i, got[i], want[i])
			}
		}
		return mustMarshal(t, &flusspb.PutKvResponse{
			BucketsResp: []*flusspb.PbPutKvRespForBucket{{
				BucketId:     proto.Int32(0),
				LogEndOffset: proto.Int64(88),
			}},
		}), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, Config{Endpoints: []string{srv.addr}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = cli.Close() }()

	schema := NewSchema(Int64Type(), StringType(), StringType())
	row, err := NewRow(schema, int64(42), nil, "diamond")
	if err != nil {
		t.Fatalf("NewRow() error = %v", err)
	}

	got, err := cli.Table(TablePath{DatabaseName: "demo", TableName: "customers"}).PartialUpdateIndexedRow(ctx, 0, row, []int32{0, 2})
	if err != nil {
		t.Fatalf("PartialUpdateIndexedRow() error = %v", err)
	}
	if len(got) != 1 || got[0].LogEndOffset != 88 {
		t.Fatalf("unexpected put results: %#v", got)
	}
}

func testValueEqual(got, want any) bool {
	switch wantValue := want.(type) {
	case nil:
		return got == nil
	case []byte:
		gotValue, ok := got.([]byte)
		return ok && bytes.Equal(gotValue, wantValue)
	default:
		return got == want
	}
}
