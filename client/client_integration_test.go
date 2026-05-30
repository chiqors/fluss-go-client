package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
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
			flusspb.ApiKey_GetTableInfo,
			flusspb.ApiKey_GetTableSchema,
			flusspb.ApiKey_ListTables,
			flusspb.ApiKey_TableExists,
			flusspb.ApiKey_ListPartitionInfos,
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
		return mustMarshal(t, metadataResponseForSingleBucket(host, int32(port), TablePath{DatabaseName: "demo", TableName: "logs"}, 21, 2)), nil
	})

	srv.on(flusspb.ApiKey_ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
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
	srv.on(flusspb.ApiKey_PutKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
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

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	if _, err := writer.Write(ctx, []BucketRecordBatch{{BucketID: 0, Records: []byte("x")}}); err == nil || err != ErrClosed {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}
