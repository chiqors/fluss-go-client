package rowcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"testing"
)

func TestEncodeLookupKeyUsesCompactedEncoding(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType(), StringType())
	row, err := NewRow(schema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		t.Fatalf("NewRow() error = %v", err)
	}

	got, err := row.EncodeLookupKey(0, 1)
	if err != nil {
		t.Fatalf("EncodeLookupKey() error = %v", err)
	}

	want := []byte{
		42,
		12,
		'A', 'd', 'a', ' ', 'L', 'o', 'v', 'e', 'l', 'a', 'c', 'e',
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeLookupKey() = %v, want %v", got, want)
	}
}

func TestIndexedRoundTripScalarsAndTemporal(t *testing.T) {
	schema := NewSchema(
		BoolType(),
		Int8Type(),
		Int16Type(),
		Int32Type(),
		Int64Type(),
		Float32Type(),
		Float64Type(),
		StringType(),
		BytesType(),
		DecimalType(10, 2),
		DateType(),
		TimeType(),
		TimestampNtzType(6),
		TimestampLtzType(6),
	)
	values := []any{
		true,
		int8(7),
		int16(9),
		int32(11),
		int64(13),
		float32(1.5),
		float64(2.5),
		"hello",
		[]byte("world"),
		Decimal{Unscaled: big.NewInt(12345), Scale: 2},
		int32(20000),
		int32(1234),
		TimestampNtz{Millisecond: 1717000000123, NanoOfMillisecond: 456789},
		TimestampLtz{EpochMillisecond: 1717000000456, NanoOfMillisecond: 123456},
	}

	payload, err := encodeRow(schema, values, true)
	if err != nil {
		t.Fatalf("encodeRow(indexed) error = %v", err)
	}
	got, err := DecodeIndexed(schema, payload)
	if err != nil {
		t.Fatalf("DecodeIndexed() error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestCompactedRoundTripScalarsAndTemporal(t *testing.T) {
	schema := NewSchema(
		BoolType(),
		Int8Type(),
		Int16Type(),
		Int32Type(),
		Int64Type(),
		Float32Type(),
		Float64Type(),
		DateType(),
		TimeType(),
	)
	values := []any{
		true,
		int8(2),
		int16(3),
		int32(4),
		int64(5),
		float32(1.25),
		float64(2.5),
		int32(20000),
		int32(1234),
	}

	payload, err := encodeRow(schema, values, false)
	if err != nil {
		t.Fatalf("encodeRow(compacted) error = %v", err)
	}
	got, err := DecodeCompacted(schema, payload)
	if err != nil {
		t.Fatalf("DecodeCompacted() error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestCompactedRoundTripWithStrings(t *testing.T) {
	schema := NewSchema(
		Int64Type(),
		StringType(),
		StringType(),
	)
	values := []any{
		int64(42),
		"Ada Lovelace",
		"gold",
	}

	payload, err := encodeRow(schema, values, false)
	if err != nil {
		t.Fatalf("encodeRow(compacted strings) error = %v", err)
	}
	got, err := DecodeCompacted(schema, payload)
	if err != nil {
		t.Fatalf("DecodeCompacted(strings) error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestCompactedRoundTripWithNullVariableField(t *testing.T) {
	schema := NewSchema(
		Int64Type(),
		StringType(),
		StringType(),
	)
	values := []any{
		int64(42),
		nil,
		"diamond",
	}

	payload, err := encodeRow(schema, values, false)
	if err != nil {
		t.Fatalf("encodeRow(compacted null variable) error = %v", err)
	}
	got, err := DecodeCompacted(schema, payload)
	if err != nil {
		t.Fatalf("DecodeCompacted(null variable) error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestIndexedRoundTripCompositeTypes(t *testing.T) {
	schema := NewSchema(
		ArrayType(Int32Type()),
		MapType(StringType(), Int64Type()),
		RowType(StringType(), Int32Type(), ArrayType(StringType())),
	)
	values := []any{
		[]any{int32(1), int32(2), int32(3)},
		map[any]any{"a": int64(10), "b": int64(20)},
		[]any{"nested", int32(8), []any{"x", "y"}},
	}

	payload, err := encodeRow(schema, values, true)
	if err != nil {
		t.Fatalf("encodeRow(indexed composite) error = %v", err)
	}
	got, err := DecodeIndexed(schema, payload)
	if err != nil {
		t.Fatalf("DecodeIndexed(composite) error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestIndexedRoundTripWithNullVariableField(t *testing.T) {
	schema := NewSchema(
		Int64Type(),
		StringType(),
		StringType(),
	)
	values := []any{
		int64(42),
		nil,
		"diamond",
	}

	payload, err := encodeRow(schema, values, true)
	if err != nil {
		t.Fatalf("encodeRow(indexed null variable) error = %v", err)
	}
	got, err := DecodeIndexed(schema, payload)
	if err != nil {
		t.Fatalf("DecodeIndexed(null variable) error = %v", err)
	}

	assertValuesEqual(t, got, values)
}

func TestEncodeKvRecordBatchIncludesLengthPrefixedKey(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType())
	payload, err := EncodeKvRecordBatch(schema, []any{int64(42), "value"}, KvBatchOptions{
		SchemaID:   1,
		Indexed:    true,
		KeyColumns: []int{0},
	})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch() error = %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("EncodeKvRecordBatch() returned empty payload")
	}
}

func TestEncodeLogRecordBatchIncludesWriterState(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType())
	payload, err := EncodeLogRecordBatch(schema, []any{int64(42), "value"}, LogBatchOptions{
		SchemaID: 9,
		Indexed:  true,
		WriterState: &WriterState{
			WriterID:      12345,
			BatchSequence: 7,
		},
	})
	if err != nil {
		t.Fatalf("EncodeLogRecordBatch() error = %v", err)
	}

	gotWriterID := int64(binary.LittleEndian.Uint64(payload[32:40]))
	if gotWriterID != 12345 {
		t.Fatalf("log writer id = %d, want %d", gotWriterID, 12345)
	}
	gotSequence := int32(binary.LittleEndian.Uint32(payload[40:44]))
	if gotSequence != 7 {
		t.Fatalf("log batch sequence = %d, want %d", gotSequence, 7)
	}
}

func TestEncodeKvRecordBatchIncludesWriterState(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType())
	payload, err := EncodeKvRecordBatch(schema, []any{int64(42), "value"}, KvBatchOptions{
		SchemaID:   9,
		Indexed:    true,
		KeyColumns: []int{0},
		WriterState: &WriterState{
			WriterID:      6789,
			BatchSequence: 11,
		},
	})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch() error = %v", err)
	}

	gotWriterID := int64(binary.LittleEndian.Uint64(payload[12:20]))
	if gotWriterID != 6789 {
		t.Fatalf("kv writer id = %d, want %d", gotWriterID, 6789)
	}
	gotSequence := int32(binary.LittleEndian.Uint32(payload[20:24]))
	if gotSequence != 11 {
		t.Fatalf("kv batch sequence = %d, want %d", gotSequence, 11)
	}
}

func TestDecodeValueRecordBatchRows(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType(), StringType())

	row1, err := NewRow(schema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		t.Fatalf("NewRow(row1) error = %v", err)
	}
	row2, err := NewRow(schema, int64(43), "Grace Hopper", "platinum")
	if err != nil {
		t.Fatalf("NewRow(row2) error = %v", err)
	}

	kv1, err := EncodeKvRecordBatch(schema, row1.Values, KvBatchOptions{SchemaID: 1, Indexed: true, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch(row1) error = %v", err)
	}
	kv2, err := EncodeKvRecordBatch(schema, row2.Values, KvBatchOptions{SchemaID: 1, Indexed: true, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch(row2) error = %v", err)
	}

	buildValueRecord := func(kvPayload []byte) []byte {
		_, recordPayload, err := decodeKvBatch(kvPayload)
		if err != nil {
			t.Fatalf("decodeKvBatch() error = %v", err)
		}
		keyLen, n := binary.Uvarint(recordPayload[4:])
		if n <= 0 {
			t.Fatalf("invalid key length varint")
		}
		rowPayload := recordPayload[4+n+int(keyLen):]
		out := make([]byte, 0, 4+2+len(rowPayload))
		out = binary.LittleEndian.AppendUint32(out, uint32(2+len(rowPayload)))
		out = binary.LittleEndian.AppendUint16(out, 1)
		out = append(out, rowPayload...)
		return out
	}

	valueRecords := append(buildValueRecord(kv1), buildValueRecord(kv2)...)
	batch := make([]byte, 0, 9+len(valueRecords))
	batch = binary.LittleEndian.AppendUint32(batch, uint32(5+len(valueRecords)))
	batch = append(batch, 0)
	batch = binary.LittleEndian.AppendUint32(batch, 2)
	batch = append(batch, valueRecords...)

	got, err := DecodeValueRecordBatchRows(schema, batch, true)
	if err != nil {
		t.Fatalf("DecodeValueRecordBatchRows() error = %v", err)
	}
	want := [][]any{row1.Values, row2.Values}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		assertValuesEqual(t, got[i], want[i])
	}
}

func TestDecodeCompactedValueRecordBatchRows(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType(), StringType())

	row1, err := NewRow(schema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		t.Fatalf("NewRow(row1) error = %v", err)
	}
	row2, err := NewRow(schema, int64(43), "Grace Hopper", "platinum")
	if err != nil {
		t.Fatalf("NewRow(row2) error = %v", err)
	}

	kv1, err := EncodeKvRecordBatch(schema, row1.Values, KvBatchOptions{SchemaID: 1, Indexed: false, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch(row1) error = %v", err)
	}
	kv2, err := EncodeKvRecordBatch(schema, row2.Values, KvBatchOptions{SchemaID: 1, Indexed: false, KeyColumns: []int{0}})
	if err != nil {
		t.Fatalf("EncodeKvRecordBatch(row2) error = %v", err)
	}

	buildValueRecord := func(kvPayload []byte) []byte {
		_, recordPayload, err := decodeKvBatch(kvPayload)
		if err != nil {
			t.Fatalf("decodeKvBatch() error = %v", err)
		}
		keyLen, n := binary.Uvarint(recordPayload[4:])
		if n <= 0 {
			t.Fatalf("invalid key length varint")
		}
		rowPayload := recordPayload[4+n+int(keyLen):]
		out := make([]byte, 0, 4+2+len(rowPayload))
		out = binary.LittleEndian.AppendUint32(out, uint32(2+len(rowPayload)))
		out = binary.LittleEndian.AppendUint16(out, 1)
		out = append(out, rowPayload...)
		return out
	}

	valueRecords := append(buildValueRecord(kv1), buildValueRecord(kv2)...)
	batch := make([]byte, 0, 9+len(valueRecords))
	batch = binary.LittleEndian.AppendUint32(batch, uint32(5+len(valueRecords)))
	batch = append(batch, 0)
	batch = binary.LittleEndian.AppendUint32(batch, 2)
	batch = append(batch, valueRecords...)

	got, err := DecodeValueRecordBatchRows(schema, batch, false)
	if err != nil {
		t.Fatalf("DecodeValueRecordBatchRows(compacted) error = %v", err)
	}
	want := [][]any{row1.Values, row2.Values}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		assertValuesEqual(t, got[i], want[i])
	}
}

func assertValuesEqual(t *testing.T, got, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("value length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !valueEqual(got[i], want[i]) {
			t.Fatalf("value[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func valueEqual(got, want any) bool {
	switch wantValue := want.(type) {
	case []byte:
		gotValue, ok := got.([]byte)
		return ok && bytes.Equal(gotValue, wantValue)
	case Decimal:
		gotValue, ok := got.(Decimal)
		return ok && gotValue.Scale == wantValue.Scale && gotValue.Unscaled.Cmp(wantValue.Unscaled) == 0
	case TimestampNtz:
		gotValue, ok := got.(TimestampNtz)
		return ok && gotValue == wantValue
	case TimestampLtz:
		gotValue, ok := got.(TimestampLtz)
		return ok && gotValue == wantValue
	case []any:
		gotValue, ok := got.([]any)
		if !ok || len(gotValue) != len(wantValue) {
			return false
		}
		for i := range wantValue {
			if !valueEqual(gotValue[i], wantValue[i]) {
				return false
			}
		}
		return true
	case map[any]any:
		gotValue, ok := got.(map[any]any)
		if !ok || len(gotValue) != len(wantValue) {
			return false
		}
		gotKeys := make([]string, 0, len(gotValue))
		wantKeys := make([]string, 0, len(wantValue))
		for key := range gotValue {
			gotKeys = append(gotKeys, stringify(key))
		}
		for key := range wantValue {
			wantKeys = append(wantKeys, stringify(key))
		}
		sort.Strings(gotKeys)
		sort.Strings(wantKeys)
		for i := range gotKeys {
			if gotKeys[i] != wantKeys[i] {
				return false
			}
		}
		for key, wantItem := range wantValue {
			gotItem, ok := gotValue[key]
			if !ok || !valueEqual(gotItem, wantItem) {
				return false
			}
		}
		return true
	default:
		return got == want
	}
}

func stringify(v any) string {
	return fmt.Sprint(v)
}
