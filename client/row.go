package client

import (
	"context"
	"fmt"
	"strings"

	arrowcodec "github.com/chiqors/fluss-go-client/internal/codec/arrow"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	"github.com/chiqors/fluss-go-client/internal/metadata"
)

// AppendIndexedRow encodes a single row using the indexed row layout and appends it to a log table.
func (t *TableClient) AppendIndexedRow(ctx context.Context, bucketID int32, row Row) ([]ProduceResult, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	payload, err := rowcodec.EncodeLogRecordBatch(row.Schema, row.Values, rowcodec.LogBatchOptions{SchemaID: info.SchemaID, Indexed: true})
	if err != nil {
		return nil, err
	}
	return t.AppendLog(ctx, -1, 15000, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// AppendArrowRows encodes one or more rows using the Arrow log layout and appends them to a log table.
func (t *TableClient) AppendArrowRows(ctx context.Context, bucketID int32, rows []Row) ([]ProduceResult, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("fluss: append arrow rows: at least one row is required")
	}
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	schema := rows[0].Schema
	values := make([][]any, 0, len(rows))
	for i, row := range rows {
		if len(row.Schema.Fields) != len(schema.Fields) {
			return nil, fmt.Errorf("fluss: append arrow rows: row %d schema field count mismatch", i)
		}
		if err := row.Schema.Validate(); err != nil {
			return nil, fmt.Errorf("fluss: append arrow rows: row %d schema invalid: %w", i, err)
		}
		values = append(values, row.Values)
	}
	properties, err := metadata.ParseTableProperties(info.JSON)
	if err != nil {
		return nil, fmt.Errorf("fluss: append arrow rows: parse table properties: %w", err)
	}
	logOpts := arrowcodec.LogBatchOptions{
		SchemaID:   info.SchemaID,
		AppendOnly: true,
	}
	switch strings.ToUpper(properties["table.log.arrow.compression.type"]) {
	case "", "ZSTD":
		logOpts.Zstd = true
	case "LZ4_FRAME":
		logOpts.LZ4 = true
	case "NONE":
	default:
		return nil, fmt.Errorf("fluss: append arrow rows: unsupported arrow compression type %q", properties["table.log.arrow.compression.type"])
	}
	payload, err := arrowcodec.EncodeLogRecordBatch(schema, values, logOpts)
	if err != nil {
		return nil, err
	}
	return t.AppendLog(ctx, -1, 15000, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// UpsertIndexedRow encodes a single row using the indexed row layout and upserts it into a KV table.
func (t *TableClient) UpsertIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	columnNames, _, primaryKeys, _, err := metadata.ParseTableDescriptor(info.JSON)
	if err != nil {
		return nil, fmt.Errorf("fluss: parse table keys: %w", err)
	}
	keyColumns, err := keyColumnsByName(columnNames, primaryKeys)
	if err != nil {
		return nil, err
	}
	payload, err := rowcodec.EncodeKvRecordBatch(row.Schema, row.Values, rowcodec.KvBatchOptions{SchemaID: info.SchemaID, Indexed: true, KeyColumns: keyColumns})
	if err != nil {
		return nil, err
	}
	if targetColumns == nil {
		targetColumns = []int32{}
	}
	return t.UpsertKV(ctx, -1, 15000, targetColumns, nil, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// PartialUpdateIndexedRow encodes a single indexed KV row and applies partial-update semantics
// for the specified target columns. Callers should provide primary-key fields plus the columns
// being updated; non-target, non-key columns should generally be left nil to mirror the upstream
// Java partial-update contract.
func (t *TableClient) PartialUpdateIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	if len(targetColumns) == 0 {
		return nil, fmt.Errorf("fluss: partial update indexed row: at least one target column is required")
	}
	return t.UpsertIndexedRow(ctx, bucketID, row, targetColumns)
}

// DeleteIndexedRow encodes a tombstone record for the row key and deletes it from a KV table.
func (t *TableClient) DeleteIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	columnNames, _, primaryKeys, _, err := metadata.ParseTableDescriptor(info.JSON)
	if err != nil {
		return nil, fmt.Errorf("fluss: parse table keys: %w", err)
	}
	keyColumns, err := keyColumnsByName(columnNames, primaryKeys)
	if err != nil {
		return nil, err
	}
	payload, err := rowcodec.EncodeKvDeleteRecordBatch(row.Schema, row.Values, rowcodec.KvBatchOptions{SchemaID: info.SchemaID, Indexed: true, KeyColumns: keyColumns})
	if err != nil {
		return nil, err
	}
	if targetColumns == nil {
		targetColumns = []int32{}
	}
	return t.UpsertKV(ctx, -1, 15000, targetColumns, nil, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

func keyColumnsByName(columnNames []string, keyNames []string) ([]int, error) {
	if len(keyNames) == 0 {
		return nil, nil
	}
	columns := make([]int, 0, len(keyNames))
	for _, keyName := range keyNames {
		idx := -1
		for i, columnName := range columnNames {
			if columnName == keyName {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("fluss: key column %q not found in table schema %s", keyName, strings.Join(columnNames, ","))
		}
		columns = append(columns, idx)
	}
	return columns, nil
}

// DecodeIndexedRowPayload decodes an indexed row payload back into field values.
func DecodeIndexedRowPayload(schema Schema, payload []byte) ([]any, error) {
	values, err := rowcodec.DecodeIndexed(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed row payload: %w", err)
	}
	return values, nil
}

// DecodeIndexedLogBatchPayload decodes a log batch payload that contains a single indexed row.
func DecodeIndexedLogBatchPayload(schema Schema, payload []byte) ([]any, error) {
	values, err := rowcodec.DecodeLogRecordBatch(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed log batch payload: %w", err)
	}
	return values, nil
}

// DecodeIndexedLogBatchRows decodes a log batch payload that contains one or more indexed rows.
func DecodeIndexedLogBatchRows(schema Schema, payload []byte) ([][]any, error) {
	values, err := rowcodec.DecodeLogRecordBatchRows(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed log batch rows: %w", err)
	}
	return values, nil
}

// DecodeArrowLogBatchRows decodes one or more Arrow-format log batches into rows.
func DecodeArrowLogBatchRows(schema Schema, payload []byte) ([][]any, error) {
	values, err := arrowcodec.DecodeLogRecordBatchRows(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode arrow log batch rows: %w", err)
	}
	return values, nil
}

// DecodeProjectedArrowLogBatchRows decodes Arrow-format log batches returned by server-side
// projection pushdown. Fluss prunes and rebuilds these batches, so their original full-batch CRC
// contract is not preserved and should not be revalidated on the client.
func DecodeProjectedArrowLogBatchRows(schema Schema, payload []byte) ([][]any, error) {
	values, err := arrowcodec.DecodeProjectedLogRecordBatchRows(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode projected arrow log batch rows: %w", err)
	}
	return values, nil
}

// DecodeIndexedValueBatchRows decodes a value-record batch payload returned by KV limit scans.
func DecodeIndexedValueBatchRows(schema Schema, payload []byte) ([][]any, error) {
	values, err := rowcodec.DecodeValueRecordBatchRows(schema, payload)
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed value batch rows: %w", err)
	}
	return values, nil
}

// DecodeIndexedLimitScanRows decodes limit-scan rows for either log or primary-key tables.
func DecodeIndexedLimitScanRows(schema Schema, result LimitScanResult) ([][]any, error) {
	if result.IsLogTable {
		return DecodeIndexedLogBatchRows(schema, result.Records)
	}
	return DecodeIndexedValueBatchRows(schema, result.Records)
}

// DecodeIndexedLookupValuePayload decodes a KV lookup value that is prefixed with a 2-byte schema id.
func DecodeIndexedLookupValuePayload(schema Schema, payload []byte) ([]any, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("fluss: decode indexed lookup value payload: data: payload too short")
	}
	values, err := rowcodec.DecodeIndexed(schema, payload[2:])
	if err != nil {
		return nil, fmt.Errorf("fluss: decode indexed lookup value payload: %w", err)
	}
	return values, nil
}

// LookupIndexedRows looks up one or more primary-key rows and decodes them into public row values.
func (t *TableClient) LookupIndexedRows(ctx context.Context, bucketID int32, schema Schema, rows []Row, keyColumns []int) ([][]any, error) {
	req := LookupBucketRequest{BucketID: bucketID}
	for i, row := range rows {
		key, err := row.EncodeLookupKey(keyColumns...)
		if err != nil {
			return nil, fmt.Errorf("fluss: lookup indexed rows: encode row %d key: %w", i, err)
		}
		req.Keys = append(req.Keys, key)
	}
	result, err := t.Lookup(ctx, []LookupBucketRequest{req}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(result) != 1 {
		return nil, fmt.Errorf("fluss: lookup indexed rows: unexpected result count %d", len(result))
	}
	if len(result[0].Values) != len(rows) {
		return nil, fmt.Errorf("fluss: lookup indexed rows: unexpected value count %d", len(result[0].Values))
	}
	out := make([][]any, 0, len(result[0].Values))
	for i, payload := range result[0].Values {
		if payload == nil {
			out = append(out, nil)
			continue
		}
		values, err := DecodeIndexedLookupValuePayload(schema, payload)
		if err != nil {
			return nil, fmt.Errorf("fluss: lookup indexed rows: decode row %d: %w", i, err)
		}
		out = append(out, values)
	}
	return out, nil
}

// LookupIndexedRow looks up a single primary-key row and returns either the decoded row or nil.
func (t *TableClient) LookupIndexedRow(ctx context.Context, bucketID int32, schema Schema, row Row, keyColumns []int) ([]any, error) {
	values, err := t.LookupIndexedRows(ctx, bucketID, schema, []Row{row}, keyColumns)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("fluss: lookup indexed row: unexpected result count %d", len(values))
	}
	return values[0], nil
}

// ScanIndexedRows drains the public KV scanner and decodes all value batches into rows.
func (t *TableClient) ScanIndexedRows(ctx context.Context, partitionID *int64, bucketID int32, limit *int64, batchSizeBytes int32, schema Schema) ([][]any, error) {
	scanner := t.NewKVScanner(partitionID, bucketID, limit, batchSizeBytes)
	defer func() { _ = scanner.Close(ctx) }()

	var out [][]any
	emptyPages := 0
	for {
		result, err := scanner.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(result.Records) == 0 {
			if !result.HasMoreResults {
				return out, nil
			}
			emptyPages++
			if emptyPages >= 2 {
				return nil, fmt.Errorf("fluss: scan indexed rows: scanner made no progress after %d empty pages", emptyPages)
			}
			continue
		}
		emptyPages = 0
		rows, err := DecodeIndexedValueBatchRows(schema, result.Records)
		if err != nil {
			return nil, fmt.Errorf("fluss: scan indexed rows: decode batch: %w", err)
		}
		out = append(out, rows...)
		if !result.HasMoreResults {
			return out, nil
		}
	}
}
