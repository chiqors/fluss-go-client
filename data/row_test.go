package data

import (
	"bytes"
	"testing"
)

func TestEncodeLookupKeyUsesCompactedEncoding(t *testing.T) {
	schema := NewSchema(Int64Type(), StringType(), StringType())
	row, err := NewRow(schema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		t.Fatalf("NewRow() error = %v", err)
	}

	got, err := row.EncodeLookupKey(0)
	if err != nil {
		t.Fatalf("EncodeLookupKey() error = %v", err)
	}

	want := []byte{42}

	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeLookupKey() = %v, want %v", got, want)
	}

	if len(got) != len(want) {
		t.Fatalf("EncodeLookupKey() length = %d, want %d for compacted key encoding", len(got), len(want))
	}
}
