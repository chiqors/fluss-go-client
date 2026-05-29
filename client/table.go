package client

import (
	"context"
	"fmt"
	"sort"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/proto"
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
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.ProduceLog, "ProduceLogRequest", "ProduceLogResponse", func(m proto.Message) error {
			req, ok := m.(*flusspb.ProduceLogRequest)
			if !ok {
				return fmt.Errorf("fluss: unexpected produce log request type %T", m)
			}
			req.Acks = proto.Int32(acks)
			req.TableId = proto.Int64(tableInfo.ID)
			req.TimeoutMs = proto.Int32(timeoutMs)
			for _, batch := range batchList {
				req.BucketsReq = append(req.BucketsReq, buildProduceBucketRecord(batch.PartitionID, batch.BucketID, batch.Records))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parseProduceResults(resp.(proto.Message))
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
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.PutKV, "PutKvRequest", "PutKvResponse", func(m proto.Message) error {
			req, ok := m.(*flusspb.PutKvRequest)
			if !ok {
				return fmt.Errorf("fluss: unexpected put kv request type %T", m)
			}
			req.Acks = proto.Int32(acks)
			req.TableId = proto.Int64(tableInfo.ID)
			req.TimeoutMs = proto.Int32(timeoutMs)
			req.TargetColumns = append(req.TargetColumns, targetColumns...)
			if aggMode != nil {
				req.AggMode = proto.Int32(*aggMode)
			}
			for _, batch := range batchList {
				req.BucketsReq = append(req.BucketsReq, buildPutBucketRecord(batch.PartitionID, batch.BucketID, batch.Records))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parsePutResults(resp.(proto.Message))
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
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.PrefixLookup, "PrefixLookupRequest", "PrefixLookupResponse", func(m proto.Message) error {
			req, ok := m.(*flusspb.PrefixLookupRequest)
			if !ok {
				return fmt.Errorf("fluss: unexpected prefix lookup request type %T", m)
			}
			req.TableId = proto.Int64(tableInfo.ID)
			for _, item := range items {
				req.BucketsReq = append(req.BucketsReq, buildPrefixLookupBucket(item))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parsePrefixLookupResults(resp.(proto.Message))
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
		resp, err := t.client.rpc.Invoke(ctx, addr, protocol.FetchLog, "FetchLogRequest", "FetchLogResponse", func(m proto.Message) error {
			req, ok := m.(*flusspb.FetchLogRequest)
			if !ok {
				return fmt.Errorf("fluss: unexpected fetch log request type %T", m)
			}
			req.FollowerServerId = proto.Int32(followerServerID)
			req.MaxBytes = proto.Int32(maxBytes)
			if maxWaitMs != nil {
				req.MaxWaitMs = proto.Int32(*maxWaitMs)
			}
			if minBytes != nil {
				req.MinBytes = proto.Int32(*minBytes)
			}
			tableReq := &flusspb.PbFetchLogReqForTable{
				TableId:                   proto.Int64(tableInfo.ID),
				ProjectionPushdownEnabled: proto.Bool(false),
			}
			for _, item := range items {
				tableReq.BucketsReq = append(tableReq.BucketsReq, buildFetchLogBucket(item))
			}
			req.TablesReq = append(req.TablesReq, tableReq)
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parseFetchResults(resp.(proto.Message))
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
	resp, err := t.client.rpc.Invoke(ctx, node.Address(), protocol.LimitScan, "LimitScanRequest", "LimitScanResponse", func(m proto.Message) error {
		req, ok := m.(*flusspb.LimitScanRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected limit scan request type %T", m)
		}
		req.TableId = proto.Int64(tableInfo.ID)
		if partitionID != nil {
			req.PartitionId = proto.Int64(*partitionID)
		}
		req.BucketId = proto.Int32(bucketID)
		req.Limit = proto.Int32(limit)
		return nil
	})
	if err != nil {
		return LimitScanResult{}, err
	}
	r, ok := resp.(*flusspb.LimitScanResponse)
	if !ok {
		return LimitScanResult{}, fmt.Errorf("fluss: unexpected limit scan response type %T", resp)
	}
	if r.GetErrorCode() != 0 {
		return LimitScanResult{}, &protocol.APIError{Code: r.GetErrorCode(), Message: r.GetErrorMessage()}
	}
	return LimitScanResult{
		IsLogTable: r.GetIsLogTable(),
		Records:    append([]byte(nil), r.GetRecords()...),
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
		resp, err := t.client.rpc.Invoke(ctx, addr, api, reqName, respName, func(m proto.Message) error {
			req, ok := m.(*flusspb.LookupRequest)
			if !ok {
				return fmt.Errorf("fluss: unexpected lookup request type %T", m)
			}
			req.TableId = proto.Int64(tableInfo.ID)
			if insertIfNotExists != nil {
				req.InsertIfNotExists = proto.Bool(*insertIfNotExists)
			}
			if acks != nil {
				req.Acks = proto.Int32(*acks)
			}
			if timeoutMs != nil {
				req.TimeoutMs = proto.Int32(*timeoutMs)
			}
			for _, item := range items {
				req.BucketsReq = append(req.BucketsReq, buildLookupBucket(item))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		results, err := parseLookupResults(resp.(proto.Message))
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}
