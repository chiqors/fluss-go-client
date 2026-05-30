//go:build rocksdb

package snapshot

import (
	"fmt"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
	"github.com/tecbot/gorocksdb"
)

type Reader struct {
	db      *gorocksdb.DB
	ro      *gorocksdb.ReadOptions
	iter    *gorocksdb.Iterator
	schemas map[int32]rowcodec.Schema
	indexed bool
}

func Open(localDir string, schemas map[int32]rowcodec.Schema, indexed bool) (*Reader, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	opts.SetErrorIfExists(false)
	opts.SetParanoidChecks(false)
	opts.SetAllowMmapReads(true)
	opts.SetMaxOpenFiles(-1)

	db, err := gorocksdb.OpenDbForReadOnly(opts, localDir, false)
	opts.Destroy()
	if err != nil {
		return nil, fmt.Errorf("snapshot: open local snapshot db: %w", err)
	}
	ro := gorocksdb.NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	return &Reader{
		db:      db,
		ro:      ro,
		iter:    iter,
		schemas: schemas,
		indexed: indexed,
	}, nil
}

func (r *Reader) ReadAll() ([][]any, error) {
	rows := make([][]any, 0)
	for r.iter.SeekToFirst(); r.iter.Valid(); r.iter.Next() {
		slice := r.iter.Value()
		value := append([]byte(nil), slice.Data()...)
		slice.Free()
		row, err := decodeValue(r.schemas, r.indexed, value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := r.iter.Err(); err != nil {
		return nil, fmt.Errorf("snapshot: iterate local snapshot db: %w", err)
	}
	return rows, nil
}

func (r *Reader) Close() error {
	if r.iter != nil {
		r.iter.Close()
	}
	if r.ro != nil {
		r.ro.Destroy()
	}
	if r.db != nil {
		r.db.Close()
	}
	return nil
}
