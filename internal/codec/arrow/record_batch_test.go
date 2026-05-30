package arrowcodec

import (
	"encoding/binary"
	"testing"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

func TestRecordBatchRoundTripSupportedTypes(t *testing.T) {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.Int32Type(),
		rowcodec.Float64Type(),
		rowcodec.StringType(),
		rowcodec.BytesType(),
		rowcodec.DateType(),
		rowcodec.TimeType(),
		rowcodec.TimestampNtzType(6),
		rowcodec.TimestampLtzType(6),
	)
	rows := [][]any{
		{
			int64(1),
			int32(101),
			float64(19.95),
			"created",
			[]byte("alpha"),
			int32(20000),
			int32(3723000),
			rowcodec.TimestampNtz{Millisecond: 1717000000123, NanoOfMillisecond: 456000},
			rowcodec.TimestampLtz{EpochMillisecond: 1717000000456, NanoOfMillisecond: 123000},
		},
		{
			int64(2),
			int32(102),
			float64(29.5),
			"packed",
			[]byte("beta"),
			int32(20001),
			int32(3724000),
			rowcodec.TimestampNtz{Millisecond: 1717000001123, NanoOfMillisecond: 0},
			rowcodec.TimestampLtz{EpochMillisecond: 1717000001456, NanoOfMillisecond: 0},
		},
	}

	payload, err := EncodeRecordBatch(schema, rows)
	if err != nil {
		t.Fatalf("EncodeRecordBatch() error = %v", err)
	}
	got, err := DecodeRecordBatch(schema, payload)
	if err != nil {
		t.Fatalf("DecodeRecordBatch() error = %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("row count = %d, want %d", len(got), len(rows))
	}
	for i := range rows {
		for j := range rows[i] {
			if !valueEqual(got[i][j], rows[i][j]) {
				t.Fatalf("row[%d][%d] = %#v, want %#v", i, j, got[i][j], rows[i][j])
			}
		}
	}
}

func TestLogRecordBatchRoundTrip(t *testing.T) {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.Int32Type(),
		rowcodec.StringType(),
	)
	rows := [][]any{
		{int64(1), int32(101), "created"},
		{int64(2), int32(102), "packed"},
	}

	payload, err := EncodeLogRecordBatch(schema, rows, LogBatchOptions{
		SchemaID:   7,
		AppendOnly: true,
	})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}
	got, err := DecodeLogRecordBatchRows(schema, payload)
	if err != nil {
		t.Fatalf("DecodeLogRecordBatchRows() error = %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("row count = %d, want %d", len(got), len(rows))
	}
	for i := range rows {
		for j := range rows[i] {
			if !valueEqual(got[i][j], rows[i][j]) {
				t.Fatalf("row[%d][%d] = %#v, want %#v", i, j, got[i][j], rows[i][j])
			}
		}
	}
}

func TestLogRecordBatchIncludesWriterState(t *testing.T) {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.Int32Type(),
		rowcodec.StringType(),
	)
	rows := [][]any{
		{int64(1), int32(101), "created"},
	}

	payload, err := EncodeLogRecordBatch(schema, rows, LogBatchOptions{
		SchemaID:   7,
		AppendOnly: true,
		WriterState: &rowcodec.WriterState{
			WriterID:      555,
			BatchSequence: 9,
		},
	})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}

	gotWriterID := int64(binary.LittleEndian.Uint64(payload[32:40]))
	if gotWriterID != 555 {
		t.Fatalf("log writer id = %d, want %d", gotWriterID, 555)
	}
	gotSequence := int32(binary.LittleEndian.Uint32(payload[40:44]))
	if gotSequence != 9 {
		t.Fatalf("log batch sequence = %d, want %d", gotSequence, 9)
	}
}

func valueEqual(got, want any) bool {
	switch w := want.(type) {
	case []byte:
		g, ok := got.([]byte)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range w {
			if g[i] != w[i] {
				return false
			}
		}
		return true
	default:
		return got == want
	}
}
