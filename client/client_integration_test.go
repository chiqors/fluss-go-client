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

	"github.com/chiqors/fluss-go-client/internal/pbutil"
	iproto "github.com/chiqors/fluss-go-client/internal/proto"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type mockFlussServer struct {
	t        *testing.T
	ln       net.Listener
	addr     string
	mu       sync.Mutex
	handlers map[protocol.APIKey]func(int32, []byte) ([]byte, error)
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
		handlers: map[protocol.APIKey]func(int32, []byte) ([]byte, error){},
	}
	go s.serve()
	return s
}

func (s *mockFlussServer) Close() {
	_ = s.ln.Close()
}

func (s *mockFlussServer) on(api protocol.APIKey, fn func(int32, []byte) ([]byte, error)) {
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

func readRequest(r io.Reader) (protocol.APIKey, int32, []byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return 0, 0, nil, err
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, 0, nil, err
	}
	apiKey := protocol.APIKey(int16(binary.BigEndian.Uint16(body[0:2])))
	reqID := int32(binary.BigEndian.Uint32(body[4:8]))
	return apiKey, reqID, body[8:], nil
}

func encodeSuccessResponse(reqID int32, payload []byte) []byte {
	frameLen := 5 + len(payload)
	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	buf[4] = byte(protocol.ResponseSuccess)
	binary.BigEndian.PutUint32(buf[5:9], uint32(reqID))
	copy(buf[9:], payload)
	return buf
}

func encodeErrorResponse(reqID int32, code int32, message string) []byte {
	msg, err := iproto.NewMessage("ErrorResponse")
	if err != nil {
		panic(err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "error_code", code); err != nil {
		panic(err)
	}
	if message != "" {
		if err := pbutil.SetString(msg.ProtoReflect(), "error_message", message); err != nil {
			panic(err)
		}
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	frameLen := 5 + len(payload)
	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	buf[4] = byte(protocol.ResponseError)
	binary.BigEndian.PutUint32(buf[5:9], uint32(reqID))
	copy(buf[9:], payload)
	return buf
}

func mustMessage(t *testing.T, name string, build func(protoreflect.Message) error) []byte {
	t.Helper()
	msg, err := iproto.NewMessage(name)
	if err != nil {
		t.Fatalf("new message %s: %v", name, err)
	}
	if err := build(msg.ProtoReflect()); err != nil {
		t.Fatalf("build %s: %v", name, err)
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	return payload
}

func addAPIVersion(t *testing.T, parent protoreflect.Message, api protocol.APIKey, version int32) {
	t.Helper()
	msg, err := iproto.NewMessage("PbApiVersion")
	if err != nil {
		t.Fatalf("PbApiVersion: %v", err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "api_key", int32(api)); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "min_version", 0); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "max_version", version); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.AppendMessage(parent, "api_versions", msg.ProtoReflect()); err != nil {
		t.Fatal(err)
	}
}

func serverNodeMessage(t *testing.T, nodeID int32, host string, port int32) protoreflect.Message {
	t.Helper()
	msg, err := iproto.NewMessage("PbServerNode")
	if err != nil {
		t.Fatalf("PbServerNode: %v", err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "node_id", nodeID); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.SetString(msg.ProtoReflect(), "host", host); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.SetInt32(msg.ProtoReflect(), "port", port); err != nil {
		t.Fatal(err)
	}
	return msg.ProtoReflect()
}

func tablePathMessage(t *testing.T, db, table string) protoreflect.Message {
	t.Helper()
	msg, err := iproto.NewMessage("PbTablePath")
	if err != nil {
		t.Fatalf("PbTablePath: %v", err)
	}
	if err := pbutil.SetString(msg.ProtoReflect(), "database_name", db); err != nil {
		t.Fatal(err)
	}
	if err := pbutil.SetString(msg.ProtoReflect(), "table_name", table); err != nil {
		t.Fatal(err)
	}
	return msg.ProtoReflect()
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

	srv.on(protocol.APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		return mustMessage(t, "ApiVersionsResponse", func(m protoreflect.Message) error {
			for _, api := range []protocol.APIKey{
				protocol.APIVersions,
				protocol.GetMetadata,
				protocol.ListDatabases,
				protocol.DatabaseExists,
				protocol.GetTableInfo,
				protocol.GetTableSchema,
				protocol.ListTables,
				protocol.TableExists,
				protocol.ListPartitionInfos,
				protocol.LimitScan,
			} {
				addAPIVersion(t, m, api, 0)
			}
			return nil
		}), nil
	})

	srv.on(protocol.GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "MetadataResponse", func(m protoreflect.Message) error {
			coord := serverNodeMessage(t, 1, host, int32(port))
			if err := pbutil.SetMessage(m, "coordinator_server", coord); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(m, "tablet_servers", coord); err != nil {
				return err
			}

			tableMeta, err := iproto.NewMessage("PbTableMetadata")
			if err != nil {
				return err
			}
			tm := tableMeta.ProtoReflect()
			if err := pbutil.SetMessage(tm, "table_path", tablePathMessage(t, "demo", "events")); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "table_id", 10); err != nil {
				return err
			}
			if err := pbutil.SetInt32(tm, "schema_id", 3); err != nil {
				return err
			}
			if err := pbutil.SetBytes(tm, "table_json", []byte(`{"name":"events"}`)); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "created_time", 1); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "modified_time", 2); err != nil {
				return err
			}

			bucketMeta, err := iproto.NewMessage("PbBucketMetadata")
			if err != nil {
				return err
			}
			bm := bucketMeta.ProtoReflect()
			if err := pbutil.SetInt32(bm, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt32(bm, "leader_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendInt32(bm, "replica_id", 1); err != nil {
				return err
			}
			if err := pbutil.SetInt32(bm, "leader_epoch", 1); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(tm, "bucket_metadata", bm); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "table_metadata", tm)
		}), nil
	})

	srv.on(protocol.ListDatabases, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "ListDatabasesResponse", func(m protoreflect.Message) error {
			if err := pbutil.AppendString(m, "database_name", "demo"); err != nil {
				return err
			}
			summary, err := iproto.NewMessage("PbDatabaseSummary")
			if err != nil {
				return err
			}
			sm := summary.ProtoReflect()
			if err := pbutil.SetString(sm, "database_name", "demo"); err != nil {
				return err
			}
			if err := pbutil.SetInt64(sm, "created_time", 123); err != nil {
				return err
			}
			if err := pbutil.SetInt32(sm, "table_count", 1); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "database_summary", sm)
		}), nil
	})

	srv.on(protocol.DatabaseExists, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req, err := iproto.NewMessage("DatabaseExistsRequest")
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		field, _ := pbutil.Field(req.ProtoReflect().Descriptor(), "database_name")
		name := req.ProtoReflect().Get(field).String()
		return mustMessage(t, "DatabaseExistsResponse", func(m protoreflect.Message) error {
			return pbutil.SetBool(m, "exists", name == "demo")
		}), nil
	})

	srv.on(protocol.GetTableInfo, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "GetTableInfoResponse", func(m protoreflect.Message) error {
			if err := pbutil.SetInt64(m, "table_id", 10); err != nil {
				return err
			}
			if err := pbutil.SetInt32(m, "schema_id", 3); err != nil {
				return err
			}
			if err := pbutil.SetBytes(m, "table_json", []byte(`{"name":"events"}`)); err != nil {
				return err
			}
			if err := pbutil.SetInt64(m, "created_time", 1); err != nil {
				return err
			}
			return pbutil.SetInt64(m, "modified_time", 2)
		}), nil
	})

	srv.on(protocol.GetTableSchema, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "GetTableSchemaResponse", func(m protoreflect.Message) error {
			if err := pbutil.SetInt32(m, "schema_id", 3); err != nil {
				return err
			}
			return pbutil.SetBytes(m, "schema_json", []byte(`{"fields":[{"name":"id"}]}`))
		}), nil
	})

	srv.on(protocol.LimitScan, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req, err := iproto.NewMessage("LimitScanRequest")
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		return mustMessage(t, "LimitScanResponse", func(m protoreflect.Message) error {
			if err := pbutil.SetBool(m, "is_log_table", true); err != nil {
				return err
			}
			return pbutil.SetBytes(m, "records", []byte("batch-data"))
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

	srv.on(protocol.APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "ApiVersionsResponse", func(m protoreflect.Message) error {
			addAPIVersion(t, m, protocol.APIVersions, 0)
			addAPIVersion(t, m, protocol.GetMetadata, 0)
			addAPIVersion(t, m, protocol.ScanKV, 0)
			return nil
		}), nil
	})

	srv.on(protocol.GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "MetadataResponse", func(m protoreflect.Message) error {
			node := serverNodeMessage(t, 1, host, int32(port))
			if err := pbutil.SetMessage(m, "coordinator_server", node); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(m, "tablet_servers", node); err != nil {
				return err
			}
			tableMeta, err := iproto.NewMessage("PbTableMetadata")
			if err != nil {
				return err
			}
			tm := tableMeta.ProtoReflect()
			if err := pbutil.SetMessage(tm, "table_path", tablePathMessage(t, "demo", "kv")); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "table_id", 11); err != nil {
				return err
			}
			if err := pbutil.SetInt32(tm, "schema_id", 1); err != nil {
				return err
			}
			if err := pbutil.SetBytes(tm, "table_json", []byte(`{}`)); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "created_time", 1); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "modified_time", 1); err != nil {
				return err
			}
			bucketMeta, err := iproto.NewMessage("PbBucketMetadata")
			if err != nil {
				return err
			}
			bm := bucketMeta.ProtoReflect()
			if err := pbutil.SetInt32(bm, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt32(bm, "leader_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendInt32(bm, "replica_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(tm, "bucket_metadata", bm); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "table_metadata", tm)
		}), nil
	})

	callCount := 0
	srv.on(protocol.ScanKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req, err := iproto.NewMessage("ScanKvRequest")
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		callCount++
		return mustMessage(t, "ScanKvResponse", func(m protoreflect.Message) error {
			switch callCount {
			case 1:
				if err := pbutil.SetBytes(m, "scanner_id", []byte("scanner-1")); err != nil {
					return err
				}
				if err := pbutil.SetBool(m, "has_more_results", true); err != nil {
					return err
				}
				if err := pbutil.SetBytes(m, "records", []byte("first-batch")); err != nil {
					return err
				}
				return pbutil.SetInt64(m, "log_offset", 99)
			default:
				if err := pbutil.SetBytes(m, "scanner_id", []byte("scanner-1")); err != nil {
					return err
				}
				if err := pbutil.SetBool(m, "has_more_results", false); err != nil {
					return err
				}
				return pbutil.SetBytes(m, "records", []byte("second-batch"))
			}
		}), nil
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

	srv.on(protocol.APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "ApiVersionsResponse", func(m protoreflect.Message) error {
			addAPIVersion(t, m, protocol.APIVersions, 0)
			addAPIVersion(t, m, protocol.GetMetadata, 0)
			addAPIVersion(t, m, protocol.ProduceLog, 0)
			return nil
		}), nil
	})

	srv.on(protocol.GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "MetadataResponse", func(m protoreflect.Message) error {
			node := serverNodeMessage(t, 1, host, int32(port))
			if err := pbutil.SetMessage(m, "coordinator_server", node); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(m, "tablet_servers", node); err != nil {
				return err
			}
			tableMeta, err := iproto.NewMessage("PbTableMetadata")
			if err != nil {
				return err
			}
			tm := tableMeta.ProtoReflect()
			if err := pbutil.SetMessage(tm, "table_path", tablePathMessage(t, "demo", "logs")); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "table_id", 21); err != nil {
				return err
			}
			if err := pbutil.SetInt32(tm, "schema_id", 2); err != nil {
				return err
			}
			if err := pbutil.SetBytes(tm, "table_json", []byte(`{}`)); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "created_time", 1); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "modified_time", 1); err != nil {
				return err
			}
			bucketMeta, err := iproto.NewMessage("PbBucketMetadata")
			if err != nil {
				return err
			}
			bm := bucketMeta.ProtoReflect()
			if err := pbutil.SetInt32(bm, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt32(bm, "leader_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendInt32(bm, "replica_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(tm, "bucket_metadata", bm); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "table_metadata", tm)
		}), nil
	})

	srv.on(protocol.ProduceLog, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req, err := iproto.NewMessage("ProduceLogRequest")
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		msg := req.ProtoReflect()
		if acks := msg.Get(msg.Descriptor().Fields().ByName("acks")).Int(); acks != -1 {
			t.Fatalf("expected default acks -1, got %d", acks)
		}
		if timeout := msg.Get(msg.Descriptor().Fields().ByName("timeout_ms")).Int(); timeout != 15000 {
			t.Fatalf("expected default timeout 15000, got %d", timeout)
		}
		return mustMessage(t, "ProduceLogResponse", func(m protoreflect.Message) error {
			item, err := iproto.NewMessage("PbProduceLogRespForBucket")
			if err != nil {
				return err
			}
			im := item.ProtoReflect()
			if err := pbutil.SetInt32(im, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt64(im, "base_offset", 42); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "buckets_resp", im)
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

	srv.on(protocol.APIVersions, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "ApiVersionsResponse", func(m protoreflect.Message) error {
			addAPIVersion(t, m, protocol.APIVersions, 0)
			addAPIVersion(t, m, protocol.GetMetadata, 0)
			addAPIVersion(t, m, protocol.PutKV, 0)
			return nil
		}), nil
	})

	srv.on(protocol.GetMetadata, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		_ = payload
		return mustMessage(t, "MetadataResponse", func(m protoreflect.Message) error {
			node := serverNodeMessage(t, 1, host, int32(port))
			if err := pbutil.SetMessage(m, "coordinator_server", node); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(m, "tablet_servers", node); err != nil {
				return err
			}
			tableMeta, err := iproto.NewMessage("PbTableMetadata")
			if err != nil {
				return err
			}
			tm := tableMeta.ProtoReflect()
			if err := pbutil.SetMessage(tm, "table_path", tablePathMessage(t, "demo", "kv")); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "table_id", 31); err != nil {
				return err
			}
			if err := pbutil.SetInt32(tm, "schema_id", 5); err != nil {
				return err
			}
			if err := pbutil.SetBytes(tm, "table_json", []byte(`{}`)); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "created_time", 1); err != nil {
				return err
			}
			if err := pbutil.SetInt64(tm, "modified_time", 1); err != nil {
				return err
			}
			bucketMeta, err := iproto.NewMessage("PbBucketMetadata")
			if err != nil {
				return err
			}
			bm := bucketMeta.ProtoReflect()
			if err := pbutil.SetInt32(bm, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt32(bm, "leader_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendInt32(bm, "replica_id", 1); err != nil {
				return err
			}
			if err := pbutil.AppendMessage(tm, "bucket_metadata", bm); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "table_metadata", tm)
		}), nil
	})

	aggMode := int32(2)
	srv.on(protocol.PutKV, func(reqID int32, payload []byte) ([]byte, error) {
		_ = reqID
		req, err := iproto.NewMessage("PutKvRequest")
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, err
		}
		msg := req.ProtoReflect()
		if acks := msg.Get(msg.Descriptor().Fields().ByName("acks")).Int(); acks != 1 {
			t.Fatalf("expected configured acks 1, got %d", acks)
		}
		if timeout := msg.Get(msg.Descriptor().Fields().ByName("timeout_ms")).Int(); timeout != 9000 {
			t.Fatalf("expected configured timeout 9000, got %d", timeout)
		}
		targetColumns := msg.Get(msg.Descriptor().Fields().ByName("target_columns")).List()
		if targetColumns.Len() != 2 || targetColumns.Get(0).Int() != 1 || targetColumns.Get(1).Int() != 3 {
			t.Fatalf("unexpected target_columns: %#v", targetColumns)
		}
		if gotAgg := msg.Get(msg.Descriptor().Fields().ByName("agg_mode")).Int(); gotAgg != int64(aggMode) {
			t.Fatalf("expected agg_mode %d, got %d", aggMode, gotAgg)
		}
		return mustMessage(t, "PutKvResponse", func(m protoreflect.Message) error {
			item, err := iproto.NewMessage("PbPutKvRespForBucket")
			if err != nil {
				return err
			}
			im := item.ProtoReflect()
			if err := pbutil.SetInt32(im, "bucket_id", 0); err != nil {
				return err
			}
			if err := pbutil.SetInt64(im, "log_end_offset", 77); err != nil {
				return err
			}
			return pbutil.AppendMessage(m, "buckets_resp", im)
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
