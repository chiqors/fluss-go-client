package snapshot

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	"github.com/cockroachdb/pebble"
)

func TestReaderReadAllCompactedRows(t *testing.T) {
	dir := t.TempDir()

	db, err := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}

	schema := rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.StringType(), rowcodec.StringType())

	buildValue := func(schemaID uint16, values []any) []byte {
		rowPayload, err := rowcodec.Row{Schema: schema, Values: values}.EncodeCompacted()
		if err != nil {
			t.Fatalf("EncodeCompacted() error = %v", err)
		}
		out := make([]byte, 2, 2+len(rowPayload))
		binary.LittleEndian.PutUint16(out[:2], schemaID)
		out = append(out, rowPayload...)
		return out
	}

	if err := db.Set([]byte("k1"), buildValue(1, []any{int64(42), "Ada Lovelace", "diamond"}), pebble.Sync); err != nil {
		t.Fatalf("db.Set(k1) error = %v", err)
	}
	if err := db.Set([]byte("k2"), buildValue(1, []any{int64(43), "Grace Hopper", "platinum"}), pebble.Sync); err != nil {
		t.Fatalf("db.Set(k2) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	reader, err := Open(filepath.Join(dir, "db"), map[int32]rowcodec.Schema{
		1: schema,
	}, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = reader.Close() }()

	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count = %d, want 2", len(rows))
	}
	if rows[0][0] != int64(42) || rows[0][1] != "Ada Lovelace" || rows[0][2] != "diamond" {
		t.Fatalf("unexpected first row: %#v", rows[0])
	}
	if rows[1][0] != int64(43) || rows[1][1] != "Grace Hopper" || rows[1][2] != "platinum" {
		t.Fatalf("unexpected second row: %#v", rows[1])
	}
}
