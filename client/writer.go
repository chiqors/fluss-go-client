package client

import "context"

type AppendWriter struct {
	table  *TableClient
	opts   AppendOptions
	closed bool
}

func (w *AppendWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]ProduceResult, error) {
	if w.closed {
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	return w.table.AppendLog(ctx, opts.Acks, opts.TimeoutMs, batches)
}

func (w *AppendWriter) Close() error {
	w.closed = true
	return nil
}

type UpsertWriter struct {
	table  *TableClient
	opts   UpsertOptions
	closed bool
}

func (w *UpsertWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]PutResult, error) {
	if w.closed {
		return nil, ErrClosed
	}
	opts := w.opts.withDefaults()
	return w.table.UpsertKV(ctx, opts.Acks, opts.TimeoutMs, opts.TargetColumns, opts.AggMode, batches)
}

func (w *UpsertWriter) Close() error {
	w.closed = true
	return nil
}

func (o AppendOptions) withDefaults() AppendOptions {
	if o.Acks == 0 {
		o.Acks = -1
	}
	if o.TimeoutMs <= 0 {
		o.TimeoutMs = 15000
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
	return o
}
