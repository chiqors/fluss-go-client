package snapshot

import (
	"fmt"
	"sync"

	rockyardkv "github.com/aalhour/rockyardkv"
)

type Reader struct {
	db       rockyardkv.ReadOnlyDB
	iter     rockyardkv.Iterator
	plans    map[int32]DecodePlan
	loadPlan func(int32) (DecodePlan, error)
	plansMu  sync.Mutex
	indexed  bool
}

func Open(localDir string, plans map[int32]DecodePlan, loadPlan func(int32) (DecodePlan, error), indexed bool) (*Reader, error) {
	db, err := rockyardkv.OpenForReadOnly(localDir, nil, false)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open local snapshot db: %w", err)
	}
	iter := db.NewIterator(nil)
	return &Reader{
		db:       db,
		iter:     iter,
		plans:    plans,
		loadPlan: loadPlan,
		indexed:  indexed,
	}, nil
}

func (r *Reader) ReadAll() ([][]any, error) {
	rows := make([][]any, 0)
	for r.iter.SeekToFirst(); r.iter.Valid(); r.iter.Next() {
		value := append([]byte(nil), r.iter.Value()...)
		row, err := decodeValue(r.lookupPlan, r.indexed, value)
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

func (r *Reader) lookupPlan(schemaID int32) (DecodePlan, bool, error) {
	r.plansMu.Lock()
	plan, ok := r.plans[schemaID]
	r.plansMu.Unlock()
	if ok {
		return plan, true, nil
	}
	if r.loadPlan == nil {
		return DecodePlan{}, false, nil
	}
	loaded, err := r.loadPlan(schemaID)
	if err != nil {
		return DecodePlan{}, false, err
	}
	r.plansMu.Lock()
	r.plans[schemaID] = loaded
	r.plansMu.Unlock()
	return loaded, true, nil
}
