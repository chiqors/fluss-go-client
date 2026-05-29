package client

import (
	"context"

	"github.com/chiqors/fluss-client-go/internal/pbutil"
	iproto "github.com/chiqors/fluss-client-go/internal/proto"
	"github.com/chiqors/fluss-client-go/protocol"
	"google.golang.org/protobuf/types/dynamicpb"
)

type KVScanner struct {
	table          *TableClient
	partitionID    *int64
	bucketID       int32
	limit          *int64
	batchSizeBytes int32
	scannerID      []byte
	callSeqID      int32
	hasMore        bool
	started        bool
}

func (s *KVScanner) Next(ctx context.Context) (ScanKVResult, error) {
	info, err := s.table.ensureTableInfo(ctx)
	if err != nil {
		return ScanKVResult{}, err
	}
	node, err := s.table.client.routeFor(info.ID, s.partitionID, s.bucketID)
	if err != nil {
		return ScanKVResult{}, err
	}
	resp, err := s.table.client.rpc.Invoke(ctx, node.Address(), protocol.ScanKV, "ScanKvRequest", "ScanKvResponse", func(m *dynamicpb.Message) error {
		msg := m.ProtoReflect()
		if s.started {
			if err := pbutil.SetBytes(msg, "scanner_id", s.scannerID); err != nil {
				return err
			}
		} else {
			req, err := iproto.NewMessage("PbScanReqForBucket")
			if err != nil {
				return err
			}
			reqMsg := req.ProtoReflect()
			if err := pbutil.SetInt64(reqMsg, "table_id", info.ID); err != nil {
				return err
			}
			if s.partitionID != nil {
				if err := pbutil.SetInt64(reqMsg, "partition_id", *s.partitionID); err != nil {
					return err
				}
			}
			if err := pbutil.SetInt32(reqMsg, "bucket_id", s.bucketID); err != nil {
				return err
			}
			if s.limit != nil {
				if err := pbutil.SetInt64(reqMsg, "limit", *s.limit); err != nil {
					return err
				}
			}
			if err := pbutil.SetMessage(msg, "bucket_scan_req", reqMsg); err != nil {
				return err
			}
		}
		if err := pbutil.SetInt32(msg, "call_seq_id", s.callSeqID); err != nil {
			return err
		}
		if s.batchSizeBytes > 0 {
			if err := pbutil.SetInt32(msg, "batch_size_bytes", s.batchSizeBytes); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return ScanKVResult{}, err
	}
	s.started = true
	s.callSeqID++
	r := resp.ProtoReflect()
	result := ScanKVResult{}
	if fd := r.Descriptor().Fields().ByName("scanner_id"); fd != nil && r.Has(fd) {
		result.ScannerID = append([]byte(nil), r.Get(fd).Bytes()...)
		s.scannerID = append([]byte(nil), result.ScannerID...)
	}
	if fd := r.Descriptor().Fields().ByName("has_more_results"); fd != nil && r.Has(fd) {
		result.HasMoreResults = r.Get(fd).Bool()
		s.hasMore = result.HasMoreResults
	}
	if fd := r.Descriptor().Fields().ByName("records"); fd != nil && r.Has(fd) {
		result.Records = append([]byte(nil), r.Get(fd).Bytes()...)
	}
	if fd := r.Descriptor().Fields().ByName("log_offset"); fd != nil && r.Has(fd) {
		v := r.Get(fd).Int()
		result.LogOffset = &v
	}
	return result, nil
}

func (s *KVScanner) Close(ctx context.Context) error {
	if len(s.scannerID) == 0 {
		return nil
	}
	info, err := s.table.ensureTableInfo(ctx)
	if err != nil {
		return err
	}
	node, err := s.table.client.routeFor(info.ID, s.partitionID, s.bucketID)
	if err != nil {
		return err
	}
	_, err = s.table.client.rpc.Invoke(ctx, node.Address(), protocol.ScanKV, "ScanKvRequest", "ScanKvResponse", func(m *dynamicpb.Message) error {
		msg := m.ProtoReflect()
		if err := pbutil.SetBytes(msg, "scanner_id", s.scannerID); err != nil {
			return err
		}
		return pbutil.SetBool(msg, "close_scanner", true)
	})
	return err
}
