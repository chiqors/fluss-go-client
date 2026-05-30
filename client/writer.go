package client

import (
	"context"
	"sync"
)

type AppendWriter struct {
	mu      sync.Mutex
	table   *TableClient
	opts    AppendOptions
	pending []BucketRecordBatch
	closed  bool
}

func (w *AppendWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]ProduceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	flushNow := opts.MaxBufferedBatches <= 1
	if flushNow {
		w.mu.Unlock()
		return w.table.AppendLog(ctx, opts.Acks, opts.TimeoutMs, cloneBucketRecordBatches(batches))
	}
	if len(w.pending)+len(batches) > opts.MaxBufferedBatches {
		w.mu.Unlock()
		return nil, ErrBufferFull
	}
	w.pending = append(w.pending, cloneBucketRecordBatches(batches)...)
	w.mu.Unlock()
	return nil, nil
}

func (w *AppendWriter) Flush(ctx context.Context) ([]ProduceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()
	if len(pending) == 0 {
		return nil, nil
	}
	return w.table.AppendLog(ctx, opts.Acks, opts.TimeoutMs, pending)
}

func (w *AppendWriter) BufferedLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

func (w *AppendWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.pending = nil
	return nil
}

func (w *AppendWriter) CloseWithContext(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	flushOnClose := w.opts.withDefaults().flushOnClose()
	w.mu.Unlock()
	if flushOnClose {
		if _, err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return w.Close()
}

type UpsertWriter struct {
	mu      sync.Mutex
	table   *TableClient
	opts    UpsertOptions
	pending []BucketRecordBatch
	closed  bool
}

func (w *UpsertWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]PutResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	flushNow := opts.MaxBufferedBatches <= 1
	if flushNow {
		w.mu.Unlock()
		return w.table.UpsertKV(ctx, opts.Acks, opts.TimeoutMs, opts.TargetColumns, opts.AggMode, cloneBucketRecordBatches(batches))
	}
	if len(w.pending)+len(batches) > opts.MaxBufferedBatches {
		w.mu.Unlock()
		return nil, ErrBufferFull
	}
	w.pending = append(w.pending, cloneBucketRecordBatches(batches)...)
	w.mu.Unlock()
	return nil, nil
}

func (w *UpsertWriter) Flush(ctx context.Context) ([]PutResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()
	if len(pending) == 0 {
		return nil, nil
	}
	return w.table.UpsertKV(ctx, opts.Acks, opts.TimeoutMs, opts.TargetColumns, opts.AggMode, pending)
}

func (w *UpsertWriter) BufferedLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

func (w *UpsertWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.pending = nil
	return nil
}

func (w *UpsertWriter) CloseWithContext(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	flushOnClose := w.opts.withDefaults().flushOnClose()
	w.mu.Unlock()
	if flushOnClose {
		if _, err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return w.Close()
}

func (o AppendOptions) withDefaults() AppendOptions {
	if o.Acks == 0 {
		o.Acks = -1
	}
	if o.TimeoutMs <= 0 {
		o.TimeoutMs = 15000
	}
	if o.MaxBufferedBatches <= 0 {
		o.MaxBufferedBatches = 1
	}
	return o
}

func (o UpsertOptions) withDefaults() UpsertOptions {
	if o.Acks == 0 {
		o.Acks = -1
	}
	if o.TimeoutMs <= 0 {
		o.TimeoutMs = 15000
	}
	if o.TargetColumns == nil {
		o.TargetColumns = []int32{}
	}
	if o.MaxBufferedBatches <= 0 {
		o.MaxBufferedBatches = 1
	}
	return o
}

func (o AppendOptions) flushOnClose() bool {
	if o.FlushOnClose == nil {
		return true
	}
	return *o.FlushOnClose
}

func (o UpsertOptions) flushOnClose() bool {
	if o.FlushOnClose == nil {
		return true
	}
	return *o.FlushOnClose
}

func cloneBucketRecordBatches(batches []BucketRecordBatch) []BucketRecordBatch {
	out := make([]BucketRecordBatch, len(batches))
	for i, batch := range batches {
		out[i] = BucketRecordBatch{
			BucketID: batch.BucketID,
			Records:  append([]byte(nil), batch.Records...),
		}
		if batch.PartitionID != nil {
			partitionID := *batch.PartitionID
			out[i].PartitionID = &partitionID
		}
	}
	return out
}
