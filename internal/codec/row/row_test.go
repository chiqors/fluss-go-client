package rowcodec

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

	got, err := row.EncodeLookupKey(0, 1)
	if err != nil {
		t.Fatalf("EncodeLookupKey() error = %v", err)
	}

	want := []byte{42, 'A', 'd', 'a', ' ', 'L', 'o', 'v', 'e', 'l', 'a', 'c', 'e'}

	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeLookupKey() = %v, want %v", got, want)
	}

	if len(got) != len(want) {
		t.Fatalf("EncodeLookupKey() length = %d, want %d for compacted key encoding", len(got), len(want))
	}
}
