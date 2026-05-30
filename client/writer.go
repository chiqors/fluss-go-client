package client

import (
	"context"
	"sync"
	"time"
)

type appendWriteFn func(context.Context, int32, int32, []BucketRecordBatch) ([]ProduceResult, error)
type upsertWriteFn func(context.Context, int32, int32, []int32, *int32, []BucketRecordBatch) ([]PutResult, error)

type appendWriterState struct {
	mu            sync.Mutex
	cond          *sync.Cond
	writeFn       appendWriteFn
	opts          AppendOptions
	pending       []BucketRecordBatch
	pendingBytes  int
	closed        bool
	timer         *time.Timer
	flushInFlight bool
}

type AppendWriter struct {
	table *TableClient
	state *appendWriterState
}

func (w *AppendWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]ProduceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	opts := w.state.opts.withDefaults()
	cloned := cloneBucketRecordBatches(batches)
	bytesAdded := bucketRecordBatchBytes(cloned)

	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	for {
		if w.state.closed {
			return nil, ErrClosed
		}
		flushNow := shouldFlushImmediately(opts)
		if flushNow {
			w.state.mu.Unlock()
			results, err := w.state.writeFn(ctx, opts.Acks, opts.TimeoutMs, cloned)
			w.state.mu.Lock()
			return results, err
		}
		if w.state.hasCapacityLocked(opts, len(cloned), bytesAdded) {
			w.state.pending = append(w.state.pending, cloned...)
			w.state.pendingBytes += bytesAdded
			shouldSchedule := opts.Linger > 0 && !w.state.flushInFlight
			if shouldSchedule {
				w.state.resetTimerLocked(w)
			}
			return nil, nil
		}
		if !opts.BlockOnBufferFull {
			return nil, ErrBufferFull
		}
		if err := waitOnAppendCond(ctx, w.state); err != nil {
			return nil, err
		}
	}
}

func (w *AppendWriter) Flush(ctx context.Context) ([]ProduceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := w.state.opts.withDefaults()
	pending, bytesPending, err := w.state.beginFlush()
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		w.state.finishFlush()
		return nil, nil
	}

	results, flushErr := w.state.writeFn(ctx, opts.Acks, opts.TimeoutMs, pending)
	if flushErr != nil {
		w.state.restorePending(pending, bytesPending)
		return results, flushErr
	}
	w.state.finishFlush()
	return results, nil
}

func (w *AppendWriter) BufferedLen() int {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	return len(w.state.pending)
}

func (w *AppendWriter) BufferedBytes() int {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	return w.state.pendingBytes
}

func (w *AppendWriter) Close() error {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.closed = true
	w.state.stopTimerLocked()
	w.state.pending = nil
	w.state.pendingBytes = 0
	w.state.broadcastLocked()
	return nil
}

func (w *AppendWriter) CloseWithContext(ctx context.Context) error {
	w.state.mu.Lock()
	if w.state.closed {
		w.state.mu.Unlock()
		return nil
	}
	flushOnClose := w.state.opts.withDefaults().flushOnClose()
	w.state.mu.Unlock()
	if flushOnClose {
		if _, err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return w.Close()
}

func (s *appendWriterState) beginFlush() ([]BucketRecordBatch, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	s.stopTimerLocked()
	if s.flushInFlight {
		return nil, 0, context.DeadlineExceeded
	}
	pending := s.pending
	pendingBytes := s.pendingBytes
	s.pending = nil
	s.pendingBytes = 0
	s.flushInFlight = true
	return pending, pendingBytes, nil
}

func (s *appendWriterState) restorePending(batches []BucketRecordBatch, bytes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(batches, s.pending...)
	s.pendingBytes += bytes
	s.flushInFlight = false
	if s.opts.withDefaults().Linger > 0 && !s.closed {
		s.resetTimerLocked(&AppendWriter{state: s})
	}
	s.broadcastLocked()
}

func (s *appendWriterState) finishFlush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushInFlight = false
	if s.opts.withDefaults().Linger > 0 && len(s.pending) > 0 && !s.closed {
		s.resetTimerLocked(&AppendWriter{state: s})
	}
	s.broadcastLocked()
}

func (s *appendWriterState) resetTimerLocked(w *AppendWriter) {
	s.stopTimerLocked()
	if s.opts.withDefaults().Linger <= 0 {
		return
	}
	s.timer = time.AfterFunc(s.opts.withDefaults().Linger, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.opts.withDefaults().TimeoutMs)*time.Millisecond)
		defer cancel()
		_, _ = w.Flush(ctx)
	})
}

func (s *appendWriterState) stopTimerLocked() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *appendWriterState) hasCapacityLocked(opts AppendOptions, batchCount int, bytesAdded int) bool {
	if opts.MaxBufferedBatches > 0 && len(s.pending)+batchCount > opts.MaxBufferedBatches {
		return false
	}
	if opts.MaxBufferedBytes > 0 && s.pendingBytes+bytesAdded > opts.MaxBufferedBytes {
		return false
	}
	return true
}

func (s *appendWriterState) broadcastLocked() {
	if s.cond != nil {
		s.cond.Broadcast()
	}
}

type upsertWriterState struct {
	mu            sync.Mutex
	cond          *sync.Cond
	writeFn       upsertWriteFn
	opts          UpsertOptions
	pending       []BucketRecordBatch
	pendingBytes  int
	closed        bool
	timer         *time.Timer
	flushInFlight bool
}

type UpsertWriter struct {
	table *TableClient
	state *upsertWriterState
}

func (w *UpsertWriter) Write(ctx context.Context, batches []BucketRecordBatch) ([]PutResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, nil
	}
	opts := w.state.opts.withDefaults()
	cloned := cloneBucketRecordBatches(batches)
	bytesAdded := bucketRecordBatchBytes(cloned)

	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	for {
		if w.state.closed {
			return nil, ErrClosed
		}
		flushNow := shouldFlushImmediatelyUpsert(opts)
		if flushNow {
			w.state.mu.Unlock()
			results, err := w.state.writeFn(ctx, opts.Acks, opts.TimeoutMs, opts.TargetColumns, opts.AggMode, cloned)
			w.state.mu.Lock()
			return results, err
		}
		if w.state.hasCapacityLocked(opts, len(cloned), bytesAdded) {
			w.state.pending = append(w.state.pending, cloned...)
			w.state.pendingBytes += bytesAdded
			shouldSchedule := opts.Linger > 0 && !w.state.flushInFlight
			if shouldSchedule {
				w.state.resetTimerLocked(w)
			}
			return nil, nil
		}
		if !opts.BlockOnBufferFull {
			return nil, ErrBufferFull
		}
		if err := waitOnUpsertCond(ctx, w.state); err != nil {
			return nil, err
		}
	}
}

func (w *UpsertWriter) Flush(ctx context.Context) ([]PutResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := w.state.opts.withDefaults()
	pending, bytesPending, err := w.state.beginFlush()
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		w.state.finishFlush()
		return nil, nil
	}

	results, flushErr := w.state.writeFn(ctx, opts.Acks, opts.TimeoutMs, opts.TargetColumns, opts.AggMode, pending)
	if flushErr != nil {
		w.state.restorePending(pending, bytesPending)
		return results, flushErr
	}
	w.state.finishFlush()
	return results, nil
}

func (w *UpsertWriter) BufferedLen() int {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	return len(w.state.pending)
}

func (w *UpsertWriter) BufferedBytes() int {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	return w.state.pendingBytes
}

func (w *UpsertWriter) Close() error {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	w.state.closed = true
	w.state.stopTimerLocked()
	w.state.pending = nil
	w.state.pendingBytes = 0
	w.state.broadcastLocked()
	return nil
}

func (w *UpsertWriter) CloseWithContext(ctx context.Context) error {
	w.state.mu.Lock()
	if w.state.closed {
		w.state.mu.Unlock()
		return nil
	}
	flushOnClose := w.state.opts.withDefaults().flushOnClose()
	w.state.mu.Unlock()
	if flushOnClose {
		if _, err := w.Flush(ctx); err != nil {
			return err
		}
	}
	return w.Close()
}

func (s *upsertWriterState) beginFlush() ([]BucketRecordBatch, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	s.stopTimerLocked()
	if s.flushInFlight {
		return nil, 0, context.DeadlineExceeded
	}
	pending := s.pending
	pendingBytes := s.pendingBytes
	s.pending = nil
	s.pendingBytes = 0
	s.flushInFlight = true
	return pending, pendingBytes, nil
}

func (s *upsertWriterState) restorePending(batches []BucketRecordBatch, bytes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(batches, s.pending...)
	s.pendingBytes += bytes
	s.flushInFlight = false
	if s.opts.withDefaults().Linger > 0 && !s.closed {
		s.resetTimerLocked(&UpsertWriter{state: s})
	}
	s.broadcastLocked()
}

func (s *upsertWriterState) finishFlush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushInFlight = false
	if s.opts.withDefaults().Linger > 0 && len(s.pending) > 0 && !s.closed {
		s.resetTimerLocked(&UpsertWriter{state: s})
	}
	s.broadcastLocked()
}

func (s *upsertWriterState) resetTimerLocked(w *UpsertWriter) {
	s.stopTimerLocked()
	if s.opts.withDefaults().Linger <= 0 {
		return
	}
	s.timer = time.AfterFunc(s.opts.withDefaults().Linger, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.opts.withDefaults().TimeoutMs)*time.Millisecond)
		defer cancel()
		_, _ = w.Flush(ctx)
	})
}

func (s *upsertWriterState) stopTimerLocked() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *upsertWriterState) hasCapacityLocked(opts UpsertOptions, batchCount int, bytesAdded int) bool {
	if opts.MaxBufferedBatches > 0 && len(s.pending)+batchCount > opts.MaxBufferedBatches {
		return false
	}
	if opts.MaxBufferedBytes > 0 && s.pendingBytes+bytesAdded > opts.MaxBufferedBytes {
		return false
	}
	return true
}

func (s *upsertWriterState) broadcastLocked() {
	if s.cond != nil {
		s.cond.Broadcast()
	}
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

func shouldFlushImmediately(opts AppendOptions) bool {
	return opts.MaxBufferedBatches <= 1 && opts.MaxBufferedBytes <= 0 && opts.Linger <= 0
}

func shouldFlushImmediatelyUpsert(opts UpsertOptions) bool {
	return opts.MaxBufferedBatches <= 1 && opts.MaxBufferedBytes <= 0 && opts.Linger <= 0
}

func waitOnAppendCond(ctx context.Context, state *appendWriterState) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			state.mu.Lock()
			state.broadcastLocked()
			state.mu.Unlock()
		case <-done:
		}
	}()
	state.cond.Wait()
	close(done)
	return ctx.Err()
}

func waitOnUpsertCond(ctx context.Context, state *upsertWriterState) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			state.mu.Lock()
			state.broadcastLocked()
			state.mu.Unlock()
		case <-done:
		}
	}()
	state.cond.Wait()
	close(done)
	return ctx.Err()
}

func bucketRecordBatchBytes(batches []BucketRecordBatch) int {
	total := 0
	for _, batch := range batches {
		total += len(batch.Records)
	}
	return total
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
