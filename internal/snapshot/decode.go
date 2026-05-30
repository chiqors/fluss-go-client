package snapshot

import (
	"encoding/binary"
	"fmt"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

type DecodePlan struct {
	DecodeSchema  rowcodec.Schema
	TargetSchema  rowcodec.Schema
	SourceColumns []string
	TargetColumns []string
}

func decodeValue(resolve func(int32) (DecodePlan, bool, error), indexed bool, value []byte) ([]any, error) {
	if len(value) < 2 {
		return nil, fmt.Errorf("snapshot: value payload too short")
	}
	schemaID := int32(binary.LittleEndian.Uint16(value[:2]))
	plan, ok, err := resolve(schemaID)
	if err != nil {
		return nil, fmt.Errorf("snapshot: load schema id %d: %w", schemaID, err)
	}
	if !ok {
		return nil, fmt.Errorf("snapshot: schema id %d not available for decode", schemaID)
	}
	var row []any
	if indexed {
		row, err = rowcodec.DecodeIndexed(plan.DecodeSchema, value[2:])
		if err != nil {
			return nil, fmt.Errorf("snapshot: decode indexed value payload: %w", err)
		}
	} else {
		row, err = rowcodec.DecodeCompacted(plan.DecodeSchema, value[2:])
		if err != nil {
			return nil, fmt.Errorf("snapshot: decode compacted value payload: %w", err)
		}
	}
	if len(plan.TargetColumns) == 0 || sameColumns(plan.SourceColumns, plan.TargetColumns) {
		return row, nil
	}
	return projectRow(row, plan.SourceColumns, plan.TargetSchema, plan.TargetColumns), nil
}

func sameColumns(source, target []string) bool {
	if len(source) != len(target) {
		return false
	}
	for i := range source {
		if source[i] != target[i] {
			return false
		}
	}
	return true
}

func projectRow(row []any, sourceColumns []string, targetSchema rowcodec.Schema, targetColumns []string) []any {
	out := make([]any, len(targetColumns))
	indexByName := make(map[string]int, len(sourceColumns))
	for i, name := range sourceColumns {
		indexByName[name] = i
	}
	for i, name := range targetColumns {
		sourceIndex, ok := indexByName[name]
		if !ok || sourceIndex >= len(row) {
			out[i] = nil
			continue
		}
		out[i] = row[sourceIndex]
	}
	return out
}
