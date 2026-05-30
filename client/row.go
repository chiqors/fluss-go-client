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
	payload, err := arrowcodec.EncodeLogRecordBatch(schema, values, arrowcodec.LogBatchOptions{
		SchemaID:   info.SchemaID,
		AppendOnly: true,
	})
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
