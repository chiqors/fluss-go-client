package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/chiqors/fluss-go-client/internal/metadata"
	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/internal/protocol"
	"google.golang.org/protobuf/proto"
)

type TableClient struct {
	client *Client
	path   TablePath
}

func (t *TableClient) NewAppendWriter(opts AppendOptions) *AppendWriter {
	state := &appendWriterState{
		writeFn: t.AppendLog,
		opts:    opts,
	}
	state.cond = sync.NewCond(&state.mu)
	return &AppendWriter{
		table: t,
		state: state,
	}
}

func (t *TableClient) NewUpsertWriter(opts UpsertOptions) *UpsertWriter {
	state := &upsertWriterState{
		writeFn: t.UpsertKV,
		opts:    opts,
	}
	state.cond = sync.NewCond(&state.mu)
	return &UpsertWriter{
		table: t,
		state: state,
	}
}

func (t *TableClient) Info(ctx context.Context) (TableInfo, error) {
	return t.client.Admin().GetTableInfo(ctx, t.path)
}

func (t *TableClient) Schema(ctx context.Context, schemaID *int32) (SchemaInfo, error) {
	return t.client.Admin().GetTableSchema(ctx, t.path, schemaID)
}

func (t *TableClient) AppendLog(ctx context.Context, acks int32, timeoutMs int32, buckets []BucketRecordBatch) ([]ProduceResult, error) {
	var allResults []ProduceResult
	pending := cloneBucketRecordBatches(buckets)
	for attempt := 0; attempt < 2; attempt++ {
		results, err := t.appendLogOnce(ctx, acks, timeoutMs, pending)
		allResults = mergeProduceResults(allResults, results)
		if err == nil {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, nil
		}
		partial, ok := err.(*PartialWriteError)
		if !ok {
			if attempt == 1 || !isRetryableWriteRouteErr(err) {
				return allResults, err
			}
			if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
				return allResults, err
			}
			continue
		}
		retryable, terminal := splitBucketWriteFailures(partial)
		if len(retryable) == 0 || attempt == 1 {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		pending = filterBucketRecordBatchesByFailure(pending, retryable)
		if len(pending) == 0 {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		if terminal != nil && len(retryable) != len(partial.Failures) {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
	}
	sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
	return allResults, nil
}

func (t *TableClient) UpsertKV(ctx context.Context, acks int32, timeoutMs int32, targetColumns []int32, aggMode *int32, buckets []BucketRecordBatch) ([]PutResult, error) {
	var allResults []PutResult
	pending := cloneBucketRecordBatches(buckets)
	for attempt := 0; attempt < 2; attempt++ {
		results, err := t.upsertKVOnce(ctx, acks, timeoutMs, targetColumns, aggMode, pending)
		allResults = mergePutResults(allResults, results)
		if err == nil {
			return allResults, nil
		}
		partial, ok := err.(*PartialWriteError)
		if !ok {
			if attempt == 1 || !isRetryableWriteRouteErr(err) {
				return allResults, err
			}
			if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
				return allResults, err
			}
			continue
		}
		retryable, terminal := splitBucketWriteFailures(partial)
		if len(retryable) == 0 || attempt == 1 {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		pending = filterBucketRecordBatchesByFailure(pending, retryable)
		if len(pending) == 0 {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
		if terminal != nil && len(retryable) != len(partial.Failures) {
			sort.Slice(allResults, func(i, j int) bool { return allResults[i].BucketID < allResults[j].BucketID })
			return allResults, terminal
		}
	}
	return allResults, nil
}

func (t *TableClient) upsertKVOnce(ctx context.Context, acks int32, timeoutMs int32, targetColumns []int32, aggMode *int32, buckets []BucketRecordBatch) ([]PutResult, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupBatchesByLeader(ctx, tableInfo.ID, buckets)
	if err != nil {
		return nil, err
	}
	var out []PutResult
	var failures []BucketWriteError
	for addr, batchList := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, flusspb.ApiKey_PutKV, "PutKvRequest", "PutKvResponse", func(m proto.Message) error {
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
		out = append(out, results...)
		if err != nil {
			partial, ok := err.(*PartialWriteError)
			if !ok {
				return out, err
			}
			failures = append(failures, partial.Failures...)
		}
	}
	if len(failures) > 0 {
		return out, &PartialWriteError{Operation: "upsert kv", Failures: failures}
	}
	return out, nil
}

func (t *TableClient) appendLogOnce(ctx context.Context, acks int32, timeoutMs int32, buckets []BucketRecordBatch) ([]ProduceResult, error) {
	tableInfo, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	grouped, err := t.groupBatchesByLeader(ctx, tableInfo.ID, buckets)
	if err != nil {
		return nil, err
	}
	var out []ProduceResult
	var failures []BucketWriteError
	for addr, batchList := range grouped {
		resp, err := t.client.rpc.Invoke(ctx, addr, flusspb.ApiKey_ProduceLog, "ProduceLogRequest", "ProduceLogResponse", func(m proto.Message) error {
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
		out = append(out, results...)
		if err != nil {
			partial, ok := err.(*PartialWriteError)
			if !ok {
				return out, err
			}
			failures = append(failures, partial.Failures...)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketID < out[j].BucketID })
	if len(failures) > 0 {
		return out, &PartialWriteError{Operation: "append log", Failures: failures}
	}
	return out, nil
}

func (t *TableClient) Lookup(ctx context.Context, reqs []LookupBucketRequest, insertIfNotExists *bool, acks *int32, timeoutMs *int32) ([]LookupBucketValues, error) {
	return t.lookupCommon(ctx, flusspb.ApiKey_Lookup, "LookupRequest", "LookupResponse", reqs, insertIfNotExists, acks, timeoutMs)
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
		resp, err := t.client.rpc.Invoke(ctx, addr, flusspb.ApiKey_PrefixLookup, "PrefixLookupRequest", "PrefixLookupResponse", func(m proto.Message) error {
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
	return t.FetchLogWithOptions(ctx, followerServerID, maxBytes, maxWaitMs, minBytes, buckets, FetchLogOptions{})
}

func (t *TableClient) FetchLogWithOptions(ctx context.Context, followerServerID int32, maxBytes int32, maxWaitMs *int32, minBytes *int32, buckets []FetchBucketRequest, opts FetchLogOptions) ([]FetchedBucket, error) {
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
		resp, err := t.client.rpc.Invoke(ctx, addr, flusspb.ApiKey_FetchLog, "FetchLogRequest", "FetchLogResponse", func(m proto.Message) error {
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
			projectionEnabled := len(opts.ProjectedFields) > 0
			tableReq := &flusspb.PbFetchLogReqForTable{
				TableId:                   proto.Int64(tableInfo.ID),
				ProjectionPushdownEnabled: proto.Bool(projectionEnabled),
			}
			if projectionEnabled {
				tableReq.ProjectedFields = append(tableReq.ProjectedFields, opts.ProjectedFields...)
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
	node, err := t.routeFor(ctx, tableInfo.ID, partitionID, bucketID)
	if err != nil {
		return LimitScanResult{}, err
	}
	resp, err := t.client.rpc.Invoke(ctx, node.Address(), flusspb.ApiKey_LimitScan, "LimitScanRequest", "LimitScanResponse", func(m proto.Message) error {
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

func (t *TableClient) routeFor(ctx context.Context, tableID int64, partitionID *int64, bucketID int32) (metadata.ServerNode, error) {
	node, err := t.client.routeFor(tableID, partitionID, bucketID)
	if err == nil {
		return node, nil
	}
	if refreshErr := t.client.RefreshMetadata(ctx, []TablePath{t.path}, nil); refreshErr != nil {
		return metadata.ServerNode{}, err
	}
	return t.client.routeFor(tableID, partitionID, bucketID)
}

func (t *TableClient) groupBatchesByLeader(ctx context.Context, tableID int64, buckets []BucketRecordBatch) (map[string][]BucketRecordBatch, error) {
	grouped := map[string][]BucketRecordBatch{}
	for _, batch := range buckets {
		node, err := t.routeFor(ctx, tableID, batch.PartitionID, batch.BucketID)
		if err != nil {
			return nil, err
		}
		grouped[node.Address()] = append(grouped[node.Address()], batch)
	}
	return grouped, nil
}

func (t *TableClient) groupLookupsByLeader(ctx context.Context, tableID int64, reqs []LookupBucketRequest) (map[string][]LookupBucketRequest, error) {
	grouped := map[string][]LookupBucketRequest{}
	for _, req := range reqs {
		node, err := t.routeFor(ctx, tableID, req.PartitionID, req.BucketID)
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
		node, err := t.routeFor(ctx, tableID, req.PartitionID, req.BucketID)
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

func (t *TableClient) lookupCommon(ctx context.Context, api flusspb.ApiKey, reqName, respName string, reqs []LookupBucketRequest, insertIfNotExists *bool, acks *int32, timeoutMs *int32) ([]LookupBucketValues, error) {
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

func isRetryableWriteRouteErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, protocol.ErrLeaderNotAvailable) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "fluss: no route for table=") ||
		strings.Contains(msg, "fluss: leader ")
}

func splitBucketWriteFailures(partial *PartialWriteError) (map[string]BucketWriteError, error) {
	retryable := make(map[string]BucketWriteError)
	terminalFailures := make([]BucketWriteError, 0)
	for _, failure := range partial.Failures {
		if isRetryableWriteRouteErr(failure.Err) {
			retryable[bucketFailureKey(failure.PartitionID, failure.BucketID)] = failure
			continue
		}
		terminalFailures = append(terminalFailures, failure)
	}
	if len(terminalFailures) == 0 {
		return retryable, nil
	}
	return retryable, &PartialWriteError{
		Operation: partial.Operation,
		Failures:  terminalFailures,
	}
}

func bucketFailureKey(partitionID *int64, bucketID int32) string {
	if partitionID == nil {
		return fmt.Sprintf("nil/%d", bucketID)
	}
	return fmt.Sprintf("%d/%d", *partitionID, bucketID)
}

func filterBucketRecordBatchesByFailure(batches []BucketRecordBatch, failures map[string]BucketWriteError) []BucketRecordBatch {
	filtered := make([]BucketRecordBatch, 0, len(batches))
	for _, batch := range batches {
		if _, ok := failures[bucketFailureKey(batch.PartitionID, batch.BucketID)]; ok {
			filtered = append(filtered, batch)
		}
	}
	return filtered
}

func mergeProduceResults(existing, current []ProduceResult) []ProduceResult {
	index := make(map[string]int, len(existing))
	out := append([]ProduceResult(nil), existing...)
	for i, result := range out {
		index[bucketFailureKey(result.PartitionID, result.BucketID)] = i
	}
	for _, result := range current {
		key := bucketFailureKey(result.PartitionID, result.BucketID)
		if i, ok := index[key]; ok {
			out[i] = result
			continue
		}
		index[key] = len(out)
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketID < out[j].BucketID })
	return out
}

func mergePutResults(existing, current []PutResult) []PutResult {
	index := make(map[string]int, len(existing))
	out := append([]PutResult(nil), existing...)
	for i, result := range out {
		index[bucketFailureKey(result.PartitionID, result.BucketID)] = i
	}
	for _, result := range current {
		key := bucketFailureKey(result.PartitionID, result.BucketID)
		if i, ok := index[key]; ok {
			out[i] = result
			continue
		}
		index[key] = len(out)
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketID < out[j].BucketID })
	return out
}
