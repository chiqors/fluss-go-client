package snapshot

import (
	"encoding/binary"
	"fmt"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

func decodeValue(schemas map[int32]rowcodec.Schema, indexed bool, value []byte) ([]any, error) {
	if len(value) < 2 {
		return nil, fmt.Errorf("snapshot: value payload too short")
	}
	schemaID := int32(binary.LittleEndian.Uint16(value[:2]))
	schema, ok := schemas[schemaID]
	if !ok {
		return nil, fmt.Errorf("snapshot: schema id %d not available for decode", schemaID)
	}
	if indexed {
		row, err := rowcodec.DecodeIndexed(schema, value[2:])
		if err != nil {
			return nil, fmt.Errorf("snapshot: decode indexed value payload: %w", err)
		}
		return row, nil
	}
	row, err := rowcodec.DecodeCompacted(schema, value[2:])
	if err != nil {
		return nil, fmt.Errorf("snapshot: decode compacted value payload: %w", err)
	}
	return row, nil
}
