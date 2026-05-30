package arrowcodec

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

const (
	logMagicV0         byte = 0
	logHeaderSize           = 48
	logLengthFieldSize      = 36

	logMagicOffset      = 12
	logCRCOffset        = 21
	logSchemaIDOffset   = 25
	logAttributesOffset = 27
	logRecordCountOff   = 44

	appendOnlyFlagMask byte = 0x01
)

type LogBatchOptions struct {
	SchemaID   int32
	AppendOnly bool
	Zstd       bool
	LZ4        bool
}

func EncodeLogRecordBatch(schema rowcodec.Schema, rows [][]any, opts LogBatchOptions) ([]byte, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("arrowcodec: at least one row is required")
	}
	recordPayload, err := EncodeRecordBatchWithOptions(schema, rows, RecordBatchEncodeOptions{
		Zstd: opts.Zstd,
		LZ4:  opts.LZ4,
	})
	if err != nil {
		return nil, err
	}
	return encodeLogBatch(recordPayload, len(rows), opts), nil
}

func DecodeLogRecordBatchRows(schema rowcodec.Schema, payload []byte) ([][]any, error) {
	return decodeLogRecordBatchRows(schema, payload, true)
}

func DecodeProjectedLogRecordBatchRows(schema rowcodec.Schema, payload []byte) ([][]any, error) {
	return decodeLogRecordBatchRows(schema, payload, false)
}

func decodeLogRecordBatchRows(schema rowcodec.Schema, payload []byte, validateCRC bool) ([][]any, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	rows := make([][]any, 0)
	for len(payload) > 0 {
		batchSize, recordPayload, _, err := decodeLogBatch(payload, validateCRC)
		if err != nil {
			return nil, err
		}
		decoded, err := DecodeRecordBatch(schema, recordPayload)
		if err != nil {
			return nil, err
		}
		rows = append(rows, decoded...)
		payload = payload[batchSize:]
	}
	return rows, nil
}

func encodeLogBatch(recordPayload []byte, recordCount int, opts LogBatchOptions) []byte {
	buf := make([]byte, 0, logHeaderSize+len(recordPayload))
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(logLengthFieldSize+len(recordPayload)))
	buf = append(buf, logMagicV0)
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(opts.SchemaID))
	if opts.AppendOnly {
		buf = append(buf, appendOnlyFlagMask)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint64(buf, ^uint64(0))
	buf = binary.LittleEndian.AppendUint32(buf, ^uint32(0))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(recordCount))
	buf = append(buf, recordPayload...)
	crc := crc32.Checksum(buf[logSchemaIDOffset:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[logCRCOffset:logSchemaIDOffset], crc)
	return buf
}

func decodeLogBatch(payload []byte, validateCRC bool) (int, []byte, bool, error) {
	if len(payload) < logHeaderSize {
		return 0, nil, false, fmt.Errorf("arrowcodec: log batch payload too short")
	}
	if payload[logMagicOffset] != logMagicV0 {
		return 0, nil, false, fmt.Errorf("arrowcodec: unsupported log batch magic %d", payload[logMagicOffset])
	}
	batchSize := int(binary.LittleEndian.Uint32(payload[8:12])) + 12
	if batchSize > len(payload) {
		return 0, nil, false, fmt.Errorf("arrowcodec: log batch payload truncated")
	}
	batch := payload[:batchSize]
	if validateCRC {
		crc := binary.LittleEndian.Uint32(batch[logCRCOffset:logSchemaIDOffset])
		check := crc32.Checksum(batch[logSchemaIDOffset:], crc32.MakeTable(crc32.Castagnoli))
		if crc != check {
			return 0, nil, false, fmt.Errorf("arrowcodec: invalid log batch crc")
		}
	}
	recordCount := int(binary.LittleEndian.Uint32(batch[logRecordCountOff : logRecordCountOff+4]))
	appendOnly := batch[logAttributesOffset]&appendOnlyFlagMask != 0
	recordPayload := batch[logHeaderSize:]
	if !appendOnly && recordCount > 0 {
		if len(recordPayload) < recordCount {
			return 0, nil, false, fmt.Errorf("arrowcodec: log batch change-type vector truncated")
		}
		recordPayload = recordPayload[recordCount:]
	}
	return batchSize, recordPayload, appendOnly, nil
}
