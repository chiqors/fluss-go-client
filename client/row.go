package client

import (
	"context"
	"fmt"
	"os"
	"strings"

	arrowcodec "github.com/chiqors/fluss-go-client/internal/codec/arrow"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	"github.com/chiqors/fluss-go-client/internal/metadata"
	"github.com/chiqors/fluss-go-client/internal/snapshot"
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

func (t *TableClient) kvEncoding(ctx context.Context) (schemaID int32, keyColumns []int, indexed bool, err error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	columnNames, _, primaryKeys, _, err := metadata.ParseTableDescriptor(info.JSON)
	if err != nil {
		return 0, nil, false, fmt.Errorf("fluss: parse table keys: %w", err)
	}
	keyColumns, err = keyColumnsByName(columnNames, primaryKeys)
	if err != nil {
		return 0, nil, false, err
	}
	properties, err := metadata.ParseTableProperties(info.JSON)
	if err != nil {
		return 0, nil, false, fmt.Errorf("fluss: parse table properties: %w", err)
	}
	indexed = !strings.EqualFold(properties["table.kv.format"], "compacted")
	return info.SchemaID, keyColumns, indexed, nil
}

func (t *TableClient) encodeKVRow(ctx context.Context, row Row) (payload []byte, indexed bool, err error) {
	schemaID, keyColumns, indexed, err := t.kvEncoding(ctx)
	if err != nil {
		return nil, false, err
	}
	payload, err = rowcodec.EncodeKvRecordBatch(row.Schema, row.Values, rowcodec.KvBatchOptions{SchemaID: schemaID, Indexed: indexed, KeyColumns: keyColumns})
	if err != nil {
		return nil, false, err
	}
	return payload, indexed, nil
}

func (t *TableClient) encodeKVDeleteRow(ctx context.Context, row Row) (payload []byte, indexed bool, err error) {
	schemaID, keyColumns, indexed, err := t.kvEncoding(ctx)
	if err != nil {
		return nil, false, err
	}
	payload, err = rowcodec.EncodeKvDeleteRecordBatch(row.Schema, row.Values, rowcodec.KvBatchOptions{SchemaID: schemaID, Indexed: indexed, KeyColumns: keyColumns})
	if err != nil {
		return nil, false, err
	}
	return payload, indexed, nil
}

// UpsertRow encodes a single row using the table KV format and upserts it into a KV table.
func (t *TableClient) UpsertRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	payload, _, err := t.encodeKVRow(ctx, row)
	if err != nil {
		return nil, err
	}
	targetColumns = normalizeTargetColumns(targetColumns, len(row.Schema.Fields))
	if targetColumns == nil {
		targetColumns = []int32{}
	}
	return t.UpsertKV(ctx, -1, 15000, targetColumns, nil, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

// UpsertIndexedRow encodes a single row using the indexed row layout and upserts it into a KV table.
func (t *TableClient) UpsertIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	return t.UpsertRow(ctx, bucketID, row, targetColumns)
}

// PartialUpdateRow encodes a single KV row and applies partial-update semantics
// for the specified target columns. Callers should provide primary-key fields plus the columns
// being updated; non-target, non-key columns should generally be left nil to mirror the upstream
// Java partial-update contract.
func (t *TableClient) PartialUpdateRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	if len(targetColumns) == 0 {
		return nil, fmt.Errorf("fluss: partial update row: at least one target column is required")
	}
	return t.UpsertRow(ctx, bucketID, row, targetColumns)
}

// PartialUpdateIndexedRow encodes a single indexed KV row and applies partial-update semantics
// for the specified target columns.
func (t *TableClient) PartialUpdateIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	return t.PartialUpdateRow(ctx, bucketID, row, targetColumns)
}

// DeleteRow encodes a tombstone record for the row key and deletes it from a KV table.
func (t *TableClient) DeleteRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	payload, _, err := t.encodeKVDeleteRow(ctx, row)
	if err != nil {
		return nil, err
	}
	targetColumns = normalizeTargetColumns(targetColumns, len(row.Schema.Fields))
	if targetColumns == nil {
		targetColumns = []int32{}
	}
	return t.UpsertKV(ctx, -1, 15000, targetColumns, nil, []BucketRecordBatch{{BucketID: bucketID, Records: payload}})
}

func normalizeTargetColumns(targetColumns []int32, fieldCount int) []int32 {
	if len(targetColumns) == 0 {
		return nil
	}
	if len(targetColumns) != fieldCount {
		return targetColumns
	}
	for i, column := range targetColumns {
		if column != int32(i) {
			return targetColumns
		}
	}
	return nil
}

// DeleteIndexedRow encodes a tombstone record for the row key and deletes it from a KV table.
func (t *TableClient) DeleteIndexedRow(ctx context.Context, bucketID int32, row Row, targetColumns []int32) ([]PutResult, error) {
	return t.DeleteRow(ctx, bucketID, row, targetColumns)
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
	values, err := rowcodec.DecodeValueRecordBatchRows(schema, payload, true)
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

// DecodeLookupValuePayload decodes a KV lookup value prefixed with a 2-byte schema id using the table KV format.
func DecodeLookupValuePayload(schema Schema, payload []byte, indexed bool) ([]any, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("fluss: decode lookup value payload: data: payload too short")
	}
	var (
		values []any
		err    error
	)
	if indexed {
		values, err = rowcodec.DecodeIndexed(schema, payload[2:])
	} else {
		values, err = rowcodec.DecodeCompacted(schema, payload[2:])
	}
	if err != nil {
		return nil, fmt.Errorf("fluss: decode lookup value payload: %w", err)
	}
	return values, nil
}

// LookupRows looks up one or more primary-key rows and decodes them into public row values.
func (t *TableClient) LookupRows(ctx context.Context, bucketID int32, schema Schema, rows []Row, keyColumns []int) ([][]any, error) {
	_, _, indexed, err := t.kvEncoding(ctx)
	if err != nil {
		return nil, err
	}
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
		values, err := DecodeLookupValuePayload(schema, payload, indexed)
		if err != nil {
			return nil, fmt.Errorf("fluss: lookup rows: decode row %d: %w", i, err)
		}
		out = append(out, values)
	}
	return out, nil
}

// LookupIndexedRows looks up one or more primary-key rows and decodes them into public row values.
func (t *TableClient) LookupIndexedRows(ctx context.Context, bucketID int32, schema Schema, rows []Row, keyColumns []int) ([][]any, error) {
	return t.LookupRows(ctx, bucketID, schema, rows, keyColumns)
}

// LookupRow looks up a single primary-key row and returns either the decoded row or nil.
func (t *TableClient) LookupRow(ctx context.Context, bucketID int32, schema Schema, row Row, keyColumns []int) ([]any, error) {
	values, err := t.LookupRows(ctx, bucketID, schema, []Row{row}, keyColumns)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("fluss: lookup row: unexpected result count %d", len(values))
	}
	return values[0], nil
}

// LookupIndexedRow looks up a single primary-key row and returns either the decoded row or nil.
func (t *TableClient) LookupIndexedRow(ctx context.Context, bucketID int32, schema Schema, row Row, keyColumns []int) ([]any, error) {
	return t.LookupRow(ctx, bucketID, schema, row, keyColumns)
}

// DecodeValueBatchRows decodes a value-record batch payload returned by KV limit scans.
func DecodeValueBatchRows(schema Schema, payload []byte, indexed bool) ([][]any, error) {
	values, err := rowcodec.DecodeValueRecordBatchRows(schema, payload, indexed)
	if err != nil {
		if indexed {
			return nil, fmt.Errorf("fluss: decode indexed value batch rows: %w", err)
		}
		return nil, fmt.Errorf("fluss: decode compacted value batch rows: %w", err)
	}
	return values, nil
}

// DecodeLimitScanRows decodes limit-scan rows for either log or primary-key tables.
func DecodeLimitScanRows(schema Schema, result LimitScanResult, indexed bool) ([][]any, error) {
	if result.IsLogTable {
		return DecodeIndexedLogBatchRows(schema, result.Records)
	}
	return DecodeValueBatchRows(schema, result.Records, indexed)
}

// ScanRows drains the public KV scanner and decodes all value batches into rows.
func (t *TableClient) ScanRows(ctx context.Context, partitionID *int64, bucketID int32, limit *int64, batchSizeBytes int32, schema Schema) ([][]any, error) {
	_, _, indexed, err := t.kvEncoding(ctx)
	if err != nil {
		return nil, err
	}
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
		rows, err := DecodeValueBatchRows(schema, result.Records, indexed)
		if err != nil {
			return nil, fmt.Errorf("fluss: scan rows: decode batch: %w", err)
		}
		out = append(out, rows...)
		if !result.HasMoreResults {
			return out, nil
		}
	}
}

// ScanIndexedRows drains the public KV scanner and decodes all value batches into rows.
func (t *TableClient) ScanIndexedRows(ctx context.Context, partitionID *int64, bucketID int32, limit *int64, batchSizeBytes int32, schema Schema) ([][]any, error) {
	return t.ScanRows(ctx, partitionID, bucketID, limit, batchSizeBytes, schema)
}

// SnapshotScanRows reads a KV snapshot for the requested bucket and decodes it into public rows.
// This mirrors the upstream Java snapshot flow: metadata RPCs, remote snapshot-file download,
// then local snapshot DB iteration. The current implementation is intentionally scoped to the
// S3-compatible storage used by the real Fluss+Paimon demo environment.
func (t *TableClient) SnapshotScanRows(ctx context.Context, schema Schema, opts SnapshotScanOptions) ([][]any, error) {
	fetcher, err := t.client.SnapshotFetcher()
	if err != nil {
		return nil, err
	}
	return t.snapshotScanRows(ctx, schema, opts, fetcher.FetchAll)
}

func (t *TableClient) snapshotScanRows(ctx context.Context, schema Schema, opts SnapshotScanOptions, fetch func(context.Context, []snapshot.RemoteFile) (string, error)) ([][]any, error) {
	info, err := t.ensureTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	_, _, indexed, err := t.kvEncoding(ctx)
	if err != nil {
		return nil, err
	}

	snapshots, err := t.client.Admin().GetLatestKvSnapshots(ctx, t.path, opts.PartitionName)
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: get latest kv snapshots: %w", err)
	}

	snapshotID := opts.SnapshotID
	if snapshotID == nil {
		snapshotID = snapshots.SnapshotIDs[opts.BucketID]
	}
	if snapshotID == nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: no snapshot id available for bucket %d", opts.BucketID)
	}

	partitionID := opts.PartitionID
	if partitionID == nil {
		partitionID = snapshots.PartitionID
	}

	metadata, err := t.client.Admin().GetKvSnapshotMetadata(ctx, snapshots.TableID, partitionID, opts.BucketID, *snapshotID)
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: get kv snapshot metadata: %w", err)
	}

	files := make([]snapshot.RemoteFile, 0, len(metadata.SnapshotFiles))
	for _, file := range metadata.SnapshotFiles {
		files = append(files, snapshot.RemoteFile{
			RemotePath:    file.RemotePath,
			LocalFileName: file.LocalFileName,
		})
	}
	localDir, err := fetch(ctx, files)
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: download snapshot files: %w", err)
	}
	defer func() { _ = os.RemoveAll(localDir) }()

	plans, loadPlan, err := t.snapshotDecodePlans(ctx, info.SchemaID, schema, opts)
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: load schemas: %w", err)
	}

	reader, err := snapshot.Open(localDir, plans, loadPlan, indexed)
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: open local snapshot: %w", err)
	}
	defer func() { _ = reader.Close() }()

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("fluss: snapshot scan rows: read local snapshot: %w", err)
	}
	return rows, nil
}

func (t *TableClient) snapshotDecodePlans(ctx context.Context, targetSchemaID int32, target Schema, opts SnapshotScanOptions) (map[int32]snapshot.DecodePlan, func(int32) (snapshot.DecodePlan, error), error) {
	targetColumns := append([]string(nil), opts.TargetColumns...)
	if len(targetColumns) == 0 {
		tableSchema, err := t.Schema(ctx, &targetSchemaID)
		if err != nil {
			return nil, nil, fmt.Errorf("get target schema %d: %w", targetSchemaID, err)
		}
		_, targetColumns, err = metadata.ParseSchema(tableSchema.JSON)
		if err != nil {
			return nil, nil, fmt.Errorf("parse target schema %d: %w", targetSchemaID, err)
		}
	}
	if len(targetColumns) != len(target.Fields) {
		return nil, nil, fmt.Errorf("target column count %d does not match target schema field count %d", len(targetColumns), len(target.Fields))
	}

	plans := map[int32]snapshot.DecodePlan{
		targetSchemaID: {
			DecodeSchema:  rowcodec.Schema(target),
			TargetSchema:  rowcodec.Schema(target),
			SourceColumns: append([]string(nil), targetColumns...),
			TargetColumns: append([]string(nil), targetColumns...),
		},
	}
	loadPlan := func(schemaID int32) (snapshot.DecodePlan, error) {
		if schemaID == targetSchemaID {
			return plans[targetSchemaID], nil
		}
		schemaInfo, err := t.Schema(ctx, &schemaID)
		if err != nil {
			return snapshot.DecodePlan{}, fmt.Errorf("get source schema %d: %w", schemaID, err)
		}
		sourceSchema, sourceColumns, err := metadata.ParseSchema(schemaInfo.JSON)
		if err != nil {
			return snapshot.DecodePlan{}, fmt.Errorf("parse source schema %d: %w", schemaID, err)
		}
		return snapshot.DecodePlan{
			DecodeSchema:  sourceSchema,
			TargetSchema:  rowcodec.Schema(target),
			SourceColumns: append([]string(nil), sourceColumns...),
			TargetColumns: append([]string(nil), targetColumns...),
		}, nil
	}
	return plans, loadPlan, nil
}
