package client

import (
	"context"
	"sort"

	"github.com/chiqors/fluss-go-client/internal/pbutil"
	iproto "github.com/chiqors/fluss-go-client/internal/proto"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type TableClient struct {
	client *Client
	path   TablePath
}

func (t *TableClient) NewAppendWriter(opts AppendOptions) *AppendWriter {
	return &AppendWriter{
		table: t,
		opts:  opts,
	}
}

func (t *TableClient) NewUpsertWriter(opts UpsertOptions) *UpsertWriter {
	return &UpsertWriter{
		table: t,
		opts:  opts,
	}
}

func (t *TableClient) Info(ctx context.Context) (TableInfo, error) {
	return t.client.Admin().GetTableInfo(ctx, t.path)
}

func (t *TableClient) Schema(ctx context.Context, schemaID *int32) (SchemaInfo, error) {
	return t.client.Admin().GetTableSchema(ctx, t.path, schemaID)
}

func (t *TableClient) AppendLog(ctx context.Context, acks int32, timeoutMs int32, buckets []BucketRecordBatch) ([]ProduceResult, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupBatchesByLeader(ctx, tableInfo.ID, buckets)
	if err != nil {
		return nil, err
	}
	var out []ProduceResult
	for addr, batchList := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.ProduceLog, "ProduceLogRequest", "ProduceLogResponse", func(m *dynamicpb.Message) error {
			msg := m.ProtoReflect()
			if err := pbutil.SetInt32(msg, "acks", acks); err != nil {
				return err
			}
			if err := pbutil.SetInt64(msg, "table_id", tableInfo.ID); err != nil {
				return err
			}
			if err := pbutil.SetInt32(msg, "timeout_ms", timeoutMs); err != nil {
				return err
			}
			for _, batch := range batchList {
				item, err := buildBucketRecord("PbProduceLogReqForBucket", batch.PartitionID, batch.BucketID, batch.Records)
				if err != nil {
					return err
				}
				if err := pbutil.AppendMessage(msg, "buckets_req", item); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parseProduceResults(resp.ProtoReflect())
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketID < out[j].BucketID })
	return out, nil
}

func (t *TableClient) UpsertKV(ctx context.Context, acks int32, timeoutMs int32, targetColumns []int32, aggMode *int32, buckets []BucketRecordBatch) ([]PutResult, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupBatchesByLeader(ctx, tableInfo.ID, buckets)
	if err != nil {
		return nil, err
	}
	var out []PutResult
	for addr, batchList := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.PutKV, "PutKvRequest", "PutKvResponse", func(m *dynamicpb.Message) error {
			msg := m.ProtoReflect()
			if err := pbutil.SetInt32(msg, "acks", acks); err != nil {
				return err
			}
			if err := pbutil.SetInt64(msg, "table_id", tableInfo.ID); err != nil {
				return err
			}
			if err := pbutil.SetInt32(msg, "timeout_ms", timeoutMs); err != nil {
				return err
			}
			if err := pbutil.AppendInt32(msg, "target_columns", targetColumns...); err != nil {
				return err
			}
			if aggMode != nil {
				if err := pbutil.SetInt32(msg, "agg_mode", *aggMode); err != nil {
					return err
				}
			}
			for _, batch := range batchList {
				item, err := buildBucketRecord("PbPutKvReqForBucket", batch.PartitionID, batch.BucketID, batch.Records)
				if err != nil {
					return err
				}
				if err := pbutil.AppendMessage(msg, "buckets_req", item); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parsePutResults(resp.ProtoReflect())
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

func (t *TableClient) Lookup(ctx context.Context, reqs []LookupBucketRequest, insertIfNotExists *bool, acks *int32, timeoutMs *int32) ([]LookupBucketValues, error) {
	return t.lookupCommon(ctx, protocol.Lookup, "LookupRequest", "LookupResponse", reqs, insertIfNotExists, acks, timeoutMs)
}

func (t *TableClient) PrefixLookup(ctx context.Context, reqs []LookupBucketRequest) ([]PrefixLookupBucketValues, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupLookupsByLeader(ctx, tableInfo.ID, reqs)
	if err != nil {
		return nil, err
	}
	var out []PrefixLookupBucketValues
	for addr, items := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.PrefixLookup, "PrefixLookupRequest", "PrefixLookupResponse", func(m *dynamicpb.Message) error {
			msg := m.ProtoReflect()
			if err := pbutil.SetInt64(msg, "table_id", tableInfo.ID); err != nil {
				return err
			}
			for _, item := range items {
				pm, err := buildLookupBucket("PbPrefixLookupReqForBucket", item)
				if err != nil {
					return err
				}
				if err := pbutil.AppendMessage(msg, "buckets_req", pm); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parsePrefixLookupResults(resp.ProtoReflect())
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

func (t *TableClient) FetchLog(ctx context.Context, followerServerID int32, maxBytes int32, maxWaitMs *int32, minBytes *int32, buckets []FetchBucketRequest) ([]FetchedBucket, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupFetchesByLeader(ctx, tableInfo.ID, buckets)
	if err != nil {
		return nil, err
	}
	var out []FetchedBucket
	for addr, items := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.FetchLog, "FetchLogRequest", "FetchLogResponse", func(m *dynamicpb.Message) error {
			msg := m.ProtoReflect()
			if err := pbutil.SetInt32(msg, "follower_server_id", followerServerID); err != nil {
				return err
			}
			if err := pbutil.SetInt32(msg, "max_bytes", maxBytes); err != nil {
				return err
			}
			if maxWaitMs != nil {
				if err := pbutil.SetInt32(msg, "max_wait_ms", *maxWaitMs); err != nil {
					return err
				}
			}
			if minBytes != nil {
				if err := pbutil.SetInt32(msg, "min_bytes", *minBytes); err != nil {
					return err
				}
			}
			tableReq, err := iproto.NewMessage("PbFetchLogReqForTable")
			if err != nil {
				return err
			}
			tableMsg := tableReq.ProtoReflect()
			if err := pbutil.SetInt64(tableMsg, "table_id", tableInfo.ID); err != nil {
				return err
			}
			if err := pbutil.SetBool(tableMsg, "projection_pushdown_enabled", false); err != nil {
				return err
			}
			for _, item := range items {
				req, err := iproto.NewMessage("PbFetchLogReqForBucket")
				if err != nil {
					return err
				}
				reqMsg := req.ProtoReflect()
				if item.PartitionID != nil {
					if err := pbutil.SetInt64(reqMsg, "partition_id", *item.PartitionID); err != nil {
						return err
					}
				}
				if err := pbutil.SetInt32(reqMsg, "bucket_id", item.BucketID); err != nil {
					return err
				}
				if err := pbutil.SetInt64(reqMsg, "fetch_offset", item.FetchOffset); err != nil {
					return err
				}
				if err := pbutil.SetInt32(reqMsg, "max_fetch_bytes", item.MaxFetchBytes); err != nil {
					return err
				}
				if err := pbutil.AppendMessage(tableMsg, "buckets_req", reqMsg); err != nil {
					return err
				}
			}
			return pbutil.AppendMessage(msg, "tables_req", tableMsg)
		})
		if err != nil {
			return nil, err
		}
		results, err := parseFetchResults(resp.ProtoReflect())
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

func (t *TableClient) LimitScan(ctx context.Context, partitionID *int64, bucketID int32, limit int32) (LimitScanResult, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return LimitScanResult{}, err
	}
	node, err := t.client.routeFor(tableInfo.ID, partitionID, bucketID)
	if err != nil {
		return LimitScanResult{}, err
	}
	resp, err := t.client.rpc.Invoke(ctx, node.Address(), protocol.LimitScan, "LimitScanRequest", "LimitScanResponse", func(m *dynamicpb.Message) error {
		msg := m.ProtoReflect()
		if err := pbutil.SetInt64(msg, "table_id", tableInfo.ID); err != nil {
			return err
		}
		if partitionID != nil {
			if err := pbutil.SetInt64(msg, "partition_id", *partitionID); err != nil {
				return err
			}
		}
		if err := pbutil.SetInt32(msg, "bucket_id", bucketID); err != nil {
			return err
		}
		return pbutil.SetInt32(msg, "limit", limit)
	})
	if err != nil {
		return LimitScanResult{}, err
	}
	r := resp.ProtoReflect()
	errorField := r.Descriptor().Fields().ByName("error_code")
	if errorField != nil && r.Has(errorField) && r.Get(errorField).Int() != 0 {
		return LimitScanResult{}, &protocol.APIError{Code: int32(r.Get(errorField).Int())}
	}
	isLogField, _ := pbutil.Field(r.Descriptor(), "is_log_table")
	recordsField, _ := pbutil.Field(r.Descriptor(), "records")
	return LimitScanResult{
		IsLogTable: r.Get(isLogField).Bool(),
		Records:    append([]byte(nil), r.Get(recordsField).Bytes()...),
	}, nil
}

func (t *TableClient) NewKVScanner(partitionID *int64, bucketID int32, limit *int64, batchSizeBytes int32) *KVScanner {
	return &KVScanner{
		table:          t,
		partitionID:    partitionID,
		bucketID:       bucketID,
		limit:          limit,
		batchSizeBytes: batchSizeBytes,
	}
}

func (t *TableClient) groupBatchesByLeader(ctx context.Context, tableID int64, buckets []BucketRecordBatch) (map[string][]BucketRecordBatch, error) {
	grouped := map[string][]BucketRecordBatch{}
	for _, batch := range buckets {
		node, err := t.client.routeFor(tableID, batch.PartitionID, batch.BucketID)
		if err != nil {
			if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
				return nil, err
			}
			node, err = t.client.routeFor(tableID, batch.PartitionID, batch.BucketID)
			if err != nil {
				return nil, err
			}
		}
		grouped[node.Address()] = append(grouped[node.Address()], batch)
	}
	return grouped, nil
}

func (t *TableClient) groupLookupsByLeader(ctx context.Context, tableID int64, reqs []LookupBucketRequest) (map[string][]LookupBucketRequest, error) {
	grouped := map[string][]LookupBucketRequest{}
	for _, req := range reqs {
		node, err := t.client.routeFor(tableID, req.PartitionID, req.BucketID)
		if err != nil {
			return nil, err
		}
		grouped[node.Address()] = append(grouped[node.Address()], req)
	}
	return grouped, nil
}

func (t *TableClient) groupFetchesByLeader(ctx context.Context, tableID int64, reqs []FetchBucketRequest) (map[string][]FetchBucketRequest, error) {
	grouped := map[string][]FetchBucketRequest{}
	for _, req := range reqs {
		node, err := t.client.routeFor(tableID, req.PartitionID, req.BucketID)
		if err != nil {
			return nil, err
		}
		grouped[node.Address()] = append(grouped[node.Address()], req)
	}
	return grouped, nil
}

func (t *TableClient) ensureTableInfo(ctx context.Context) (TableInfo, error) {
	if info, ok := t.client.metadata.Table(t.path); ok {
		return TableInfo{
			Path:         info.Path,
			ID:           info.ID,
			SchemaID:     info.SchemaID,
			JSON:         info.TableJSON,
			CreatedTime:  info.CreatedTime,
			ModifiedTime: info.ModifiedTime,
		}, nil
	}
	return t.Info(ctx)
}

func (t *TableClient) lookupCommon(ctx context.Context, api protocol.APIKey, reqName, respName string, reqs []LookupBucketRequest, insertIfNotExists *bool, acks *int32, timeoutMs *int32) ([]LookupBucketValues, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupLookupsByLeader(ctx, tableInfo.ID, reqs)
	if err != nil {
		return nil, err
	}
	var out []LookupBucketValues
	for addr, items := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, api, reqName, respName, func(m *dynamicpb.Message) error {
			msg := m.ProtoReflect()
			if err := pbutil.SetInt64(msg, "table_id", tableInfo.ID); err != nil {
				return err
			}
			if insertIfNotExists != nil {
				if err := pbutil.SetBool(msg, "insert_if_not_exists", *insertIfNotExists); err != nil {
					return err
				}
			}
			if acks != nil {
				if err := pbutil.SetInt32(msg, "acks", *acks); err != nil {
					return err
				}
			}
			if timeoutMs != nil {
				if err := pbutil.SetInt32(msg, "timeout_ms", *timeoutMs); err != nil {
					return err
				}
			}
			for _, item := range items {
				pm, err := buildLookupBucket("PbLookupReqForBucket", item)
				if err != nil {
					return err
				}
				if err := pbutil.AppendMessage(msg, "buckets_req", pm); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parseLookupResults(resp.ProtoReflect())
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

func buildLookupBucket(name string, req LookupBucketRequest) (protoreflect.Message, error) {
	msg, err := iproto.NewMessage(name)
	if err != nil {
		return nil, err
	}
	r := msg.ProtoReflect()
	if req.PartitionID != nil {
		if err := pbutil.SetInt64(r, "partition_id", *req.PartitionID); err != nil {
			return nil, err
		}
	}
	if err := pbutil.SetInt32(r, "bucket_id", req.BucketID); err != nil {
		return nil, err
	}
	if err := pbutil.AppendBytes(r, "keys", req.Keys...); err != nil {
		return nil, err
	}
	return r, nil
}
