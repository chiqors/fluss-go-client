//go:build !rocksdb

package snapshot

import (
	"fmt"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	"github.com/cockroachdb/pebble"
)

type Reader struct {
	db      *pebble.DB
	iter    *pebble.Iterator
	schemas map[int32]rowcodec.Schema
	indexed bool
}

func Open(localDir string, schemas map[int32]rowcodec.Schema, indexed bool) (*Reader, error) {
	db, err := pebble.Open(localDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("snapshot: open local snapshot db: %w", err)
	}
	iter, err := db.NewIter(nil)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("snapshot: open local snapshot iterator: %w", err)
	}
	return &Reader{
		db:      db,
		iter:    iter,
		schemas: schemas,
		indexed: indexed,
	}, nil
}

func (r *Reader) ReadAll() ([][]any, error) {
	rows := make([][]any, 0)
	for ok := r.iter.First(); ok; ok = r.iter.Next() {
		value := append([]byte(nil), r.iter.Value()...)
		row, err := decodeValue(r.schemas, r.indexed, value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := r.iter.Error(); err != nil {
		return nil, fmt.Errorf("snapshot: iterate local snapshot db: %w", err)
	}
	return rows, nil
}

func (r *Reader) Close() error {
	var firstErr error
	if r.iter != nil {
		if err := r.iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.db != nil {
		if err := r.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
