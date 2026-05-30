package client

import (
	"context"
	"fmt"

	"github.com/chiqors/fluss-go-client/data"
)

// AppendIndexedRow encodes a single row using the indexed row layout and appends it to a log table.
func (t *TableClient) AppendIndexedRow(ctx context.Context, bucketID int32, row data.Row) ([]ProduceResult, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	payload, err := data.EncodeLogRecordBatch(row.Schema, row.Values, data.LogBatchOptions{SchemaID: info.SchemaID, Indexed: true})
	if err != nil {
		return nil, err
	}
	return t.AppendLog(ctx, -1, 15000, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// UpsertIndexedRow encodes a single row using the indexed row layout and upserts it into a KV table.
func (t *TableClient) UpsertIndexedRow(ctx context.Context, bucketID int32, row data.Row, targetColumns []int32) ([]PutResult, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	payload, err := data.EncodeKvRecordBatch(row.Schema, row.Values, data.KvBatchOptions{SchemaID: info.SchemaID, Indexed: true})
	if err != nil {
		return nil, err
	}
	if targetColumns == nil {
		targetColumns = []int32{}
	}
	return t.UpsertKV(ctx, -1, 15000, targetColumns, nil, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// DecodeIndexedRowPayload decodes an indexed row payload back into field values.
func DecodeIndexedRowPayload(schema data.Schema, payload []byte) ([]any, error) {
	values, err := data.DecodeIndexed(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed row payload: %w", err)
	}
	return values, nil
}

// DecodeIndexedLogBatchPayload decodes a log batch payload that contains a single indexed row.
func DecodeIndexedLogBatchPayload(schema data.Schema, payload []byte) ([]any, error) {
	values, err := data.DecodeLogRecordBatch(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed log batch payload: %w", err)
	}
	return values, nil
}

// DecodeIndexedLookupValuePayload decodes a KV lookup value that is prefixed with a 2-byte schema id.
func DecodeIndexedLookupValuePayload(schema data.Schema, payload []byte) ([]any, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("fluss: decode indexed lookup value payload: data: payload too short")
	}
	values, err := data.DecodeIndexed(schema, payload[2:])
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed lookup value payload: %w", err)
	}
	return values, nil
}
