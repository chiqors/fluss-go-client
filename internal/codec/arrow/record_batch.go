package arrowcodec

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

func EncodeRecordBatch(schema rowcodec.Schema, rows [][]any) ([]byte, error) {
	return EncodeRecordBatchWithOptions(schema, rows)
}

type RecordBatchEncodeOptions struct {
	Zstd bool
	LZ4  bool
}

func EncodeRecordBatchWithOptions(schema rowcodec.Schema, rows [][]any, opts ...RecordBatchEncodeOptions) ([]byte, error) {
	arrowSchema, err := SchemaFromRowSchema(schema)
	if err != nil {
		return nil, err
	}
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrowSchema)
	defer builder.Release()

	for rowIdx, values := range rows {
		if len(values) != len(schema.Fields) {
			return nil, fmt.Errorf("arrowcodec: row %d: expected %d values, got %d", rowIdx, len(schema.Fields), len(values))
		}
		for colIdx, field := range schema.Fields {
			if err := appendValue(builder.Field(colIdx), field, values[colIdx]); err != nil {
				return nil, fmt.Errorf("arrowcodec: row %d field %d: %w", rowIdx, colIdx, err)
			}
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	ipcOpts := []ipc.Option{ipc.WithAllocator(mem)}
	if len(opts) > 0 {
		switch {
		case opts[0].Zstd:
			ipcOpts = append(ipcOpts, ipc.WithZstd())
		case opts[0].LZ4:
			ipcOpts = append(ipcOpts, ipc.WithLZ4())
		}
	}
	payload, err := ipc.GetRecordBatchPayload(rec, ipcOpts...)
	if err != nil {
		return nil, fmt.Errorf("arrowcodec: encode record batch payload: %w", err)
	}
	defer payload.Release()

	var out bytes.Buffer
	if _, err := payload.WritePayload(&out); err != nil {
		return nil, fmt.Errorf("arrowcodec: serialize record batch payload: %w", err)
	}
	return out.Bytes(), nil
}

func DecodeRecordBatch(schema rowcodec.Schema, payload []byte) ([][]any, error) {
	arrowSchema, err := SchemaFromRowSchema(schema)
	if err != nil {
		return nil, err
	}
	mem := memory.NewGoAllocator()
	schemaPayload := ipc.GetSchemaPayload(arrowSchema, mem)
	defer schemaPayload.Release()

	var schemaBuf bytes.Buffer
	if _, err := schemaPayload.WritePayload(&schemaBuf); err != nil {
		return nil, fmt.Errorf("arrowcodec: serialize schema payload: %w", err)
	}
	schemaMsgReader := ipc.NewMessageReader(bytes.NewReader(schemaBuf.Bytes()), ipc.WithAllocator(mem))
	defer schemaMsgReader.Release()
	schemaMsg, err := schemaMsgReader.Message()
	if err != nil {
		return nil, fmt.Errorf("arrowcodec: read schema message: %w", err)
	}

	recordMsgReader := ipc.NewMessageReader(bytes.NewReader(payload), ipc.WithAllocator(mem))
	defer recordMsgReader.Release()
	recordMsg, err := recordMsgReader.Message()
	if err != nil {
		return nil, fmt.Errorf("arrowcodec: read record batch message: %w", err)
	}

	reader, err := ipc.NewReaderFromMessageReader(newStaticMessageReader(schemaMsg, recordMsg))
	if err != nil {
		return nil, fmt.Errorf("arrowcodec: open record batch reader: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("arrowcodec: read record batch: %w", err)
		}
		return nil, nil
	}
	rec := reader.RecordBatch()
	rows := make([][]any, 0, int(rec.NumRows()))
	for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
		values := make([]any, 0, len(schema.Fields))
		for colIdx, field := range schema.Fields {
			value, err := extractValue(rec.Column(colIdx), field, rowIdx)
			if err != nil {
				return nil, fmt.Errorf("arrowcodec: row %d field %d: %w", rowIdx, colIdx, err)
			}
			values = append(values, value)
		}
		rows = append(rows, values)
	}
	return rows, nil
}

type staticMessageReader struct {
	refCount atomic.Int64
	msgs     []*ipc.Message
	idx      int
}

func newStaticMessageReader(msgs ...*ipc.Message) ipc.MessageReader {
	r := &staticMessageReader{msgs: msgs}
	r.refCount.Add(1)
	for _, msg := range msgs {
		msg.Retain()
	}
	return r
}

func (r *staticMessageReader) Message() (*ipc.Message, error) {
	if r.idx >= len(r.msgs) {
		return nil, io.EOF
	}
	msg := r.msgs[r.idx]
	r.idx++
	return msg, nil
}

func (r *staticMessageReader) Retain() {
	r.refCount.Add(1)
}

func (r *staticMessageReader) Release() {
	if r.refCount.Add(-1) == 0 {
		for _, msg := range r.msgs {
			msg.Release()
		}
		r.msgs = nil
	}
}

func appendValue(builder array.Builder, field rowcodec.FieldType, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch field.Kind {
	case rowcodec.TypeBool:
		b, ok := builder.(*array.BooleanBuilder)
		if !ok {
			return builderTypeError(builder, "*array.BooleanBuilder")
		}
		v, err := asBool(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeInt8:
		b, ok := builder.(*array.Int8Builder)
		if !ok {
			return builderTypeError(builder, "*array.Int8Builder")
		}
		v, err := asInt8(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeInt16:
		b, ok := builder.(*array.Int16Builder)
		if !ok {
			return builderTypeError(builder, "*array.Int16Builder")
		}
		v, err := asInt16(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeInt32:
		b, ok := builder.(*array.Int32Builder)
		if !ok {
			return builderTypeError(builder, "*array.Int32Builder")
		}
		v, err := asInt32(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeInt64:
		b, ok := builder.(*array.Int64Builder)
		if !ok {
			return builderTypeError(builder, "*array.Int64Builder")
		}
		v, err := asInt64(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeFloat32:
		b, ok := builder.(*array.Float32Builder)
		if !ok {
			return builderTypeError(builder, "*array.Float32Builder")
		}
		v, err := asFloat32(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeFloat64:
		b, ok := builder.(*array.Float64Builder)
		if !ok {
			return builderTypeError(builder, "*array.Float64Builder")
		}
		v, err := asFloat64(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeString:
		b, ok := builder.(*array.StringBuilder)
		if !ok {
			return builderTypeError(builder, "*array.StringBuilder")
		}
		v, err := asString(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeBytes:
		b, ok := builder.(*array.BinaryBuilder)
		if !ok {
			return builderTypeError(builder, "*array.BinaryBuilder")
		}
		v, err := asBytes(value)
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case rowcodec.TypeDecimal:
		b, ok := builder.(*array.Decimal128Builder)
		if !ok {
			return builderTypeError(builder, "*array.Decimal128Builder")
		}
		v, err := asDecimal(value, field)
		if err != nil {
			return err
		}
		b.Append(decimal128.FromBigInt(v.Unscaled))
		return nil
	case rowcodec.TypeDate:
		b, ok := builder.(*array.Date32Builder)
		if !ok {
			return builderTypeError(builder, "*array.Date32Builder")
		}
		v, err := asInt32(value)
		if err != nil {
			return err
		}
		b.Append(arrow.Date32(v))
		return nil
	case rowcodec.TypeTime:
		b, ok := builder.(*array.Time32Builder)
		if !ok {
			return builderTypeError(builder, "*array.Time32Builder")
		}
		v, err := asInt32(value)
		if err != nil {
			return err
		}
		b.Append(arrow.Time32(v))
		return nil
	case rowcodec.TypeTimestampNtz:
		b, ok := builder.(*array.TimestampBuilder)
		if !ok {
			return builderTypeError(builder, "*array.TimestampBuilder")
		}
		v, err := asTimestampNtz(value)
		if err != nil {
			return err
		}
		b.Append(timestampValueForField(field, v.Millisecond, v.NanoOfMillisecond))
		return nil
	case rowcodec.TypeTimestampLtz:
		b, ok := builder.(*array.TimestampBuilder)
		if !ok {
			return builderTypeError(builder, "*array.TimestampBuilder")
		}
		v, err := asTimestampLtz(value)
		if err != nil {
			return err
		}
		b.Append(timestampValueForField(field, v.EpochMillisecond, v.NanoOfMillisecond))
		return nil
	default:
		return fmt.Errorf("arrowcodec: unsupported type %q", field.Kind)
	}
}

func extractValue(arr arrow.Array, field rowcodec.FieldType, rowIdx int) (any, error) {
	if arr.IsNull(rowIdx) {
		return nil, nil
	}
	switch field.Kind {
	case rowcodec.TypeBool:
		return arr.(*array.Boolean).Value(rowIdx), nil
	case rowcodec.TypeInt8:
		return arr.(*array.Int8).Value(rowIdx), nil
	case rowcodec.TypeInt16:
		return arr.(*array.Int16).Value(rowIdx), nil
	case rowcodec.TypeInt32:
		return arr.(*array.Int32).Value(rowIdx), nil
	case rowcodec.TypeInt64:
		return arr.(*array.Int64).Value(rowIdx), nil
	case rowcodec.TypeFloat32:
		return arr.(*array.Float32).Value(rowIdx), nil
	case rowcodec.TypeFloat64:
		return arr.(*array.Float64).Value(rowIdx), nil
	case rowcodec.TypeString:
		return arr.(*array.String).Value(rowIdx), nil
	case rowcodec.TypeBytes:
		return append([]byte(nil), arr.(*array.Binary).Value(rowIdx)...), nil
	case rowcodec.TypeDecimal:
		v := arr.(*array.Decimal128).Value(rowIdx)
		bigValue := v.BigInt()
		return rowcodec.Decimal{Unscaled: bigValue, Scale: field.Scale}, nil
	case rowcodec.TypeDate:
		return int32(arr.(*array.Date32).Value(rowIdx)), nil
	case rowcodec.TypeTime:
		return int32(arr.(*array.Time32).Value(rowIdx)), nil
	case rowcodec.TypeTimestampNtz:
		ts := arr.(*array.Timestamp).Value(rowIdx)
		return decodeTimestampNtz(field, ts), nil
	case rowcodec.TypeTimestampLtz:
		ts := arr.(*array.Timestamp).Value(rowIdx)
		return decodeTimestampLtz(field, ts), nil
	default:
		return nil, fmt.Errorf("arrowcodec: unsupported type %q", field.Kind)
	}
}

func builderTypeError(builder array.Builder, want string) error {
	return fmt.Errorf("arrowcodec: builder %T, want %s", builder, want)
}

func timestampValueForField(field rowcodec.FieldType, millis int64, nanosOfMilli int32) arrow.Timestamp {
	t := time.UnixMilli(millis).UTC().Add(time.Duration(nanosOfMilli) * time.Nanosecond)
	switch {
	case field.Length <= 0:
		return arrow.Timestamp(t.Unix())
	case field.Length <= 3:
		return arrow.Timestamp(t.UnixMilli())
	case field.Length <= 6:
		return arrow.Timestamp(t.UnixMicro())
	default:
		return arrow.Timestamp(t.UnixNano())
	}
}

func decodeTimestampNtz(field rowcodec.FieldType, ts arrow.Timestamp) rowcodec.TimestampNtz {
	millis, nanos := timestampParts(field, ts)
	return rowcodec.TimestampNtz{Millisecond: millis, NanoOfMillisecond: nanos}
}

func decodeTimestampLtz(field rowcodec.FieldType, ts arrow.Timestamp) rowcodec.TimestampLtz {
	millis, nanos := timestampParts(field, ts)
	return rowcodec.TimestampLtz{EpochMillisecond: millis, NanoOfMillisecond: nanos}
}

func timestampParts(field rowcodec.FieldType, ts arrow.Timestamp) (int64, int32) {
	switch {
	case field.Length <= 0:
		millis := int64(ts) * 1000
		return millis, 0
	case field.Length <= 3:
		return int64(ts), 0
	case field.Length <= 6:
		micros := int64(ts)
		millis := micros / 1000
		nanos := int32((micros % 1000) * 1000)
		return millis, nanos
	default:
		nanosTotal := int64(ts)
		millis := nanosTotal / int64(time.Millisecond)
		nanos := int32(nanosTotal % int64(time.Millisecond))
		return millis, nanos
	}
}

func asBool(v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", v)
	}
	return b, nil
}

func asInt8(v any) (int8, error) {
	x, ok := v.(int8)
	if !ok {
		return 0, fmt.Errorf("expected int8, got %T", v)
	}
	return x, nil
}

func asInt16(v any) (int16, error) {
	x, ok := v.(int16)
	if !ok {
		return 0, fmt.Errorf("expected int16, got %T", v)
	}
	return x, nil
}

func asInt32(v any) (int32, error) {
	x, ok := v.(int32)
	if !ok {
		return 0, fmt.Errorf("expected int32, got %T", v)
	}
	return x, nil
}

func asInt64(v any) (int64, error) {
	x, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64, got %T", v)
	}
	return x, nil
}

func asFloat32(v any) (float32, error) {
	x, ok := v.(float32)
	if !ok {
		return 0, fmt.Errorf("expected float32, got %T", v)
	}
	return x, nil
}

func asFloat64(v any) (float64, error) {
	x, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", v)
	}
	return x, nil
}

func asString(v any) (string, error) {
	x, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", v)
	}
	return x, nil
}

func asBytes(v any) ([]byte, error) {
	x, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, got %T", v)
	}
	return append([]byte(nil), x...), nil
}

func asDecimal(v any, field rowcodec.FieldType) (rowcodec.Decimal, error) {
	x, ok := v.(rowcodec.Decimal)
	if !ok {
		return rowcodec.Decimal{}, fmt.Errorf("expected Decimal, got %T", v)
	}
	if x.Unscaled == nil {
		return rowcodec.Decimal{}, fmt.Errorf("decimal unscaled is nil")
	}
	if x.Scale != field.Scale {
		return rowcodec.Decimal{}, fmt.Errorf("decimal scale %d, want %d", x.Scale, field.Scale)
	}
	limitDigits := field.Length
	if digits := len(new(big.Int).Abs(x.Unscaled).String()); digits > limitDigits {
		return rowcodec.Decimal{}, fmt.Errorf("decimal precision %d exceeds limit %d", digits, limitDigits)
	}
	return x, nil
}

func asTimestampNtz(v any) (rowcodec.TimestampNtz, error) {
	x, ok := v.(rowcodec.TimestampNtz)
	if !ok {
		return rowcodec.TimestampNtz{}, fmt.Errorf("expected TimestampNtz, got %T", v)
	}
	return x, nil
}

func asTimestampLtz(v any) (rowcodec.TimestampLtz, error) {
	x, ok := v.(rowcodec.TimestampLtz)
	if !ok {
		return rowcodec.TimestampLtz{}, fmt.Errorf("expected TimestampLtz, got %T", v)
	}
	return x, nil
}
