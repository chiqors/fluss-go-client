package rowcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	logMagicV0         byte = 0
	kvMagicV0          byte = 0
	logHeaderSize           = 48
	kvHeaderSize            = 28
	logLengthFieldSize      = 36
	kvLengthFieldSize       = 24
)

type LogBatchOptions struct {
	SchemaID int32
	Indexed  bool
}

type KvBatchOptions struct {
	SchemaID   int32
	Indexed    bool
	KeyColumns []int
}

type Value struct {
	Raw any
}

type Row struct {
	Schema Schema
	Values []any
}

func NewRow(schema Schema, values ...any) (Row, error) {
	if len(values) != len(schema.Fields) {
		return Row{}, fmt.Errorf("rowcodec: expected %d values, got %d", len(schema.Fields), len(values))
	}
	if err := schema.Validate(); err != nil {
		return Row{}, err
	}
	return Row{Schema: schema, Values: append([]any(nil), values...)}, nil
}

func (r Row) EncodeIndexed() ([]byte, error)   { return encodeRow(r.Schema, r.Values, true) }
func (r Row) EncodeCompacted() ([]byte, error) { return encodeRow(r.Schema, r.Values, false) }

func (r Row) EncodeLookupKey(columns ...int) ([]byte, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("rowcodec: at least one key column is required")
	}
	buf := make([]byte, 0, 32)
	for _, idx := range columns {
		if idx < 0 || idx >= len(r.Values) {
			return nil, fmt.Errorf("rowcodec: key column index %d out of range", idx)
		}
		part, err := encodeCompactedKeyValue(r.Schema.Fields[idx], r.Values[idx])
		if err != nil {
			return nil, err
		}
		buf = append(buf, part...)
	}
	return buf, nil
}

func EncodeLogRecordBatch(schema Schema, values []any, opts LogBatchOptions) ([]byte, error) {
	recordPayload, err := encodeLogRecord(schema, values, opts.Indexed)
	if err != nil {
		return nil, err
	}
	return encodeLogBatch(opts.SchemaID, recordPayload), nil
}

func EncodeKvRecordBatch(schema Schema, values []any, opts KvBatchOptions) ([]byte, error) {
	recordPayload, err := encodeKvRecord(schema, values, opts)
	if err != nil {
		return nil, err
	}
	return encodeKvBatch(opts.SchemaID, recordPayload), nil
}

func DecodeLogRecordBatch(schema Schema, payload []byte) ([]any, error) {
	_, recordPayload, err := decodeLogBatch(payload)
	if err != nil {
		return nil, err
	}
	return decodeLogRecord(schema, recordPayload)
}

func DecodeLogRecordBatchRows(schema Schema, payload []byte) ([][]any, error) {
	rows := make([][]any, 0)
	for len(payload) > 0 {
		batchSize, recordPayload, err := decodeLogBatch(payload)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeLogRecords(schema, recordPayload)
		if err != nil {
			return nil, err
		}
		rows = append(rows, decoded...)
		payload = payload[batchSize:]
	}
	return rows, nil
}

func DecodeKvRecordBatch(schema Schema, payload []byte) ([]any, error) {
	_, recordPayload, err := decodeKvBatch(payload)
	if err != nil {
		return nil, err
	}
	return decodeRow(schema, recordPayload, true)
}

func encodeLogBatch(schemaID int32, recordPayload []byte) []byte {
	buf := make([]byte, 0, logHeaderSize+len(recordPayload))
	buf = binary.LittleEndian.AppendUint64(buf, 0) // base offset
	buf = binary.LittleEndian.AppendUint32(buf, uint32(logLengthFieldSize+len(recordPayload)))
	buf = append(buf, logMagicV0)
	buf = binary.LittleEndian.AppendUint64(buf, 0) // commit timestamp
	buf = binary.LittleEndian.AppendUint32(buf, 0) // crc placeholder
	buf = binary.LittleEndian.AppendUint16(buf, uint16(schemaID))
	buf = append(buf, 0)                           // attributes
	buf = binary.LittleEndian.AppendUint32(buf, 0) // last offset delta
	buf = binary.LittleEndian.AppendUint64(buf, ^uint64(0))
	buf = binary.LittleEndian.AppendUint32(buf, ^uint32(0))
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	buf = append(buf, recordPayload...)
	crc := crc32.Checksum(buf[25:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[21:25], crc)
	return buf
}

func encodeKvBatch(schemaID int32, recordPayload []byte) []byte {
	buf := make([]byte, 0, kvHeaderSize+len(recordPayload))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(kvLengthFieldSize+len(recordPayload)))
	buf = append(buf, kvMagicV0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // crc placeholder
	buf = binary.LittleEndian.AppendUint16(buf, uint16(schemaID))
	buf = append(buf, 0) // attributes
	buf = binary.LittleEndian.AppendUint64(buf, ^uint64(0))
	buf = binary.LittleEndian.AppendUint32(buf, ^uint32(0))
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	buf = append(buf, recordPayload...)
	crc := crc32.Checksum(buf[9:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[5:9], crc)
	return buf
}

func encodeLogRecord(schema Schema, values []any, indexed bool) ([]byte, error) {
	rowPayload, err := encodeRow(schema, values, indexed)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 5+len(rowPayload))
	out = binary.LittleEndian.AppendUint32(out, uint32(1+len(rowPayload)))
	out = append(out, 0) // attributes
	out = append(out, rowPayload...)
	return out, nil
}

func decodeLogRecord(schema Schema, payload []byte) ([]any, error) {
	rows, err := decodeLogRecords(schema, payload)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("data: log record payload empty")
	}
	return rows[0], nil
}

func decodeLogRecords(schema Schema, payload []byte) ([][]any, error) {
	rows := make([][]any, 0, 1)
	for off := 0; off < len(payload); {
		if off+5 > len(payload) {
			return nil, fmt.Errorf("data: log record payload truncated")
		}
		declared := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		recordEnd := off + 4 + declared
		if recordEnd > len(payload) {
			return nil, fmt.Errorf("data: log record payload truncated")
		}
		if payload[off+4] != 0 {
			return nil, fmt.Errorf("data: unsupported log record attributes %d", payload[off+4])
		}
		row, err := decodeRow(schema, payload[off+5:recordEnd], true)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		off = recordEnd
	}
	return rows, nil
}

func encodeKvRecord(schema Schema, values []any, opts KvBatchOptions) ([]byte, error) {
	if len(schema.Fields) == 0 {
		return nil, fmt.Errorf("rowcodec: kv schema has no fields")
	}
	row, err := NewRow(schema, values...)
	if err != nil {
		return nil, err
	}
	keyColumns := opts.KeyColumns
	if len(keyColumns) == 0 {
		keyColumns = []int{0}
	}
	keyPayload, err := row.EncodeLookupKey(keyColumns...)
	if err != nil {
		return nil, err
	}
	rowPayload, err := encodeRow(schema, values, opts.Indexed)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 4+len(keyPayload)+len(rowPayload))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(keyPayload)+len(rowPayload)))
	out = binary.AppendUvarint(out, uint64(len(keyPayload)))
	out = append(out, keyPayload...)
	out = append(out, rowPayload...)
	return out, nil
}

func encodeRow(schema Schema, values []any, indexed bool) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if len(values) != len(schema.Fields) {
		return nil, fmt.Errorf("rowcodec: expected %d values, got %d", len(schema.Fields), len(values))
	}

	nullBits := make([]byte, nullBitsSize(len(values)))
	var variableLengths []byte
	payload := make([]byte, 0, 128)
	for i, field := range schema.Fields {
		if values[i] == nil {
			setNullBit(nullBits, i)
			continue
		}
		encoded, err := encodeValue(field, values[i], indexed)
		if err != nil {
			return nil, fmt.Errorf("rowcodec: field %d: %w", i, err)
		}
		if indexed && !field.IsFixed() {
			variableLengths = binary.LittleEndian.AppendUint32(variableLengths, uint32(len(encoded)))
		}
		payload = append(payload, encoded...)
	}

	out := make([]byte, 0, len(nullBits)+len(variableLengths)+len(payload))
	out = append(out, nullBits...)
	if indexed {
		out = append(out, variableLengths...)
	}
	out = append(out, payload...)
	return out, nil
}

func encodeLookupValue(field FieldType, value any) ([]byte, error) {
	return encodeCompactedKeyValue(field, value)
}

func encodeCompactedKeyValue(field FieldType, value any) ([]byte, error) {
	switch field.Kind {
	case TypeBool:
		v, err := asBool(value)
		if err != nil {
			return nil, err
		}
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case TypeInt32:
		v, err := asInt32(value)
		if err != nil {
			return nil, err
		}
		return encodeVarint32(v), nil
	case TypeInt64:
		v, err := asInt64(value)
		if err != nil {
			return nil, err
		}
		return encodeVarint64(v), nil
	case TypeFloat64:
		v, err := asFloat64(value)
		if err != nil {
			return nil, err
		}
		return encodeFixed64(int64(math.Float64bits(v))), nil
	case TypeString:
		v, err := asString(value)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 0, 4+len(v))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
		return out, nil
	case TypeBytes:
		v, err := asBytes(value)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 0, 4+len(v))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
		return out, nil
	default:
		return nil, fmt.Errorf("data: unsupported lookup key type %q", field.Kind)
	}
}

func encodeValue(field FieldType, value any, indexed bool) ([]byte, error) {
	switch field.Kind {
	case TypeBool:
		v, err := asBool(value)
		if err != nil {
			return nil, err
		}
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case TypeInt32:
		v, err := asInt32(value)
		if err != nil {
			return nil, err
		}
		if indexed {
			return encodeFixed32(v), nil
		}
		return encodeVarint32(v), nil
	case TypeInt64:
		v, err := asInt64(value)
		if err != nil {
			return nil, err
		}
		if indexed {
			return encodeFixed64(v), nil
		}
		return encodeVarint64(v), nil
	case TypeFloat64:
		v, err := asFloat64(value)
		if err != nil {
			return nil, err
		}
		return encodeFixed64(int64(math.Float64bits(v))), nil
	case TypeString:
		v, err := asString(value)
		if err != nil {
			return nil, err
		}
		if indexed {
			out := make([]byte, 0, 4+len(v))
			out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
			out = append(out, v...)
			return out, nil
		}
		return append([]byte(nil), v...), nil
	case TypeBytes:
		v, err := asBytes(value)
		if err != nil {
			return nil, err
		}
		if indexed {
			out := make([]byte, 0, 4+len(v))
			out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
			out = append(out, v...)
			return out, nil
		}
		return append([]byte(nil), v...), nil
	case TypeDecimal:
		return nil, fmt.Errorf("data: decimal encoding not implemented yet")
	default:
		return nil, fmt.Errorf("data: unsupported type %q", field.Kind)
	}
}

func encodeFixed32(v int32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(v))
	return out
}

func encodeFixed64(v int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v))
	return out
}

func encodeVarint32(v int32) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(buf, uint64(v))
	return append([]byte(nil), buf[:n]...)
}

func encodeVarint64(v int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	return append([]byte(nil), buf[:n]...)
}

func nullBitsSize(fieldCount int) int { return (fieldCount + 7) / 8 }

func DecodeIndexed(schema Schema, payload []byte) ([]any, error) {
	return decodeRow(schema, payload, true)
}
func DecodeCompacted(schema Schema, payload []byte) ([]any, error) {
	return decodeRow(schema, payload, false)
}

func decodeLogBatch(payload []byte) (int, []byte, error) {
	if len(payload) < logHeaderSize {
		return 0, nil, fmt.Errorf("data: log batch payload too short")
	}
	if payload[12] != logMagicV0 {
		return 0, nil, fmt.Errorf("data: unsupported log batch magic %d", payload[12])
	}
	batchSize := int(binary.LittleEndian.Uint32(payload[8:12])) + 12
	if batchSize > len(payload) {
		return 0, nil, fmt.Errorf("data: log batch payload truncated")
	}
	batch := payload[:batchSize]
	crc := binary.LittleEndian.Uint32(batch[21:25])
	check := crc32.Checksum(batch[25:], crc32.MakeTable(crc32.Castagnoli))
	if crc != check {
		return 0, nil, fmt.Errorf("data: invalid log batch crc")
	}
	return batchSize, batch[logHeaderSize:], nil
}

func decodeKvBatch(payload []byte) (int32, []byte, error) {
	if len(payload) < kvHeaderSize {
		return 0, nil, fmt.Errorf("data: kv batch payload too short")
	}
	if payload[4] != kvMagicV0 {
		return 0, nil, fmt.Errorf("data: unsupported kv batch magic %d", payload[4])
	}
	crc := binary.LittleEndian.Uint32(payload[5:9])
	check := crc32.Checksum(payload[9:], crc32.MakeTable(crc32.Castagnoli))
	if crc != check {
		return 0, nil, fmt.Errorf("data: invalid kv batch crc")
	}
	return int32(binary.LittleEndian.Uint16(payload[9:11])), payload[kvHeaderSize:], nil
}

func decodeRow(schema Schema, payload []byte, indexed bool) ([]any, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	values := make([]any, len(schema.Fields))
	if len(payload) < nullBitsSize(len(schema.Fields)) {
		return nil, fmt.Errorf("data: payload too short")
	}
	nullBits := payload[:nullBitsSize(len(schema.Fields))]
	off := len(nullBits)
	var lens []uint32
	if indexed {
		lens = make([]uint32, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			if field.IsFixed() {
				lens = append(lens, uint32(fixedSize(field)))
			} else {
				if off+4 > len(payload) {
					return nil, fmt.Errorf("data: payload truncated")
				}
				lens = append(lens, binary.LittleEndian.Uint32(payload[off:off+4]))
				off += 4
			}
		}
	}
	for i, field := range schema.Fields {
		if nullBits[i/8]&(1<<uint(i%8)) != 0 {
			continue
		}
		var n int
		if indexed {
			n = int(lens[i])
		} else if field.IsFixed() {
			n = fixedSize(field)
		} else {
			return nil, fmt.Errorf("data: compacted decoding for variable field %d not implemented", i)
		}
		if off+n > len(payload) {
			return nil, fmt.Errorf("data: payload truncated")
		}
		v, err := decodeValue(field, payload[off:off+n], indexed)
		if err != nil {
			return nil, err
		}
		values[i] = v
		off += n
	}
	return values, nil
}

func fixedSize(field FieldType) int {
	switch field.Kind {
	case TypeBool:
		return 1
	case TypeInt32:
		return 4
	case TypeInt64:
		return 8
	case TypeFloat64:
		return 8
	default:
		return 0
	}
}

func decodeValue(field FieldType, payload []byte, indexed bool) (any, error) {
	switch field.Kind {
	case TypeBool:
		return len(payload) > 0 && payload[0] != 0, nil
	case TypeInt32:
		if len(payload) < 4 {
			return nil, fmt.Errorf("data: int32 payload too short")
		}
		return int32(binary.LittleEndian.Uint32(payload[:4])), nil
	case TypeInt64:
		if len(payload) < 8 {
			return nil, fmt.Errorf("data: int64 payload too short")
		}
		return int64(binary.LittleEndian.Uint64(payload[:8])), nil
	case TypeFloat64:
		if len(payload) < 8 {
			return nil, fmt.Errorf("data: float64 payload too short")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(payload[:8])), nil
	case TypeString:
		if indexed {
			if len(payload) < 4 {
				return nil, fmt.Errorf("data: string payload too short")
			}
			return string(bytes.Clone(payload[4:])), nil
		}
		return string(bytes.Clone(payload)), nil
	case TypeBytes:
		if indexed {
			if len(payload) < 4 {
				return nil, fmt.Errorf("data: bytes payload too short")
			}
			return append([]byte(nil), payload[4:]...), nil
		}
		return append([]byte(nil), payload...), nil
	default:
		return nil, fmt.Errorf("data: unsupported type %q", field.Kind)
	}
}

func setNullBit(bits []byte, index int) { bits[index/8] |= 1 << uint(index%8) }

func asBool(v any) (bool, error) {
	value, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", v)
	}
	return value, nil
}

func asInt32(v any) (int32, error) {
	switch value := v.(type) {
	case int32:
		return value, nil
	case int:
		return int32(value), nil
	case int64:
		return int32(value), nil
	default:
		return 0, fmt.Errorf("expected int32-compatible value, got %T", v)
	}
}

func asInt64(v any) (int64, error) {
	switch value := v.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	default:
		return 0, fmt.Errorf("expected int64-compatible value, got %T", v)
	}
}

func asFloat64(v any) (float64, error) {
	value, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", v)
	}
	return value, nil
}

func asString(v any) ([]byte, error) {
	switch value := v.(type) {
	case string:
		return []byte(value), nil
	case []byte:
		return append([]byte(nil), value...), nil
	default:
		return nil, fmt.Errorf("expected string or []byte, got %T", v)
	}
}

func asBytes(v any) ([]byte, error) {
	value, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, got %T", v)
	}
	return append([]byte(nil), value...), nil
}
