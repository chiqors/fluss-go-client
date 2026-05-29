package client

import (
	"context"
	"fmt"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/protocol"
	"google.golang.org/protobuf/proto"
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
	resp, err := s.table.client.rpc.Invoke(ctx, node.Address(), protocol.ScanKV, "ScanKvRequest", "ScanKvResponse", func(m proto.Message) error {
		req, ok := m.(*flusspb.ScanKvRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected scan kv request type %T", m)
		}
		if s.started {
			req.ScannerId = append([]byte(nil), s.scannerID...)
		} else {
			bucketReq := &flusspb.PbScanReqForBucket{
				TableId:  proto.Int64(info.ID),
				BucketId: proto.Int32(s.bucketID),
			}
			if s.partitionID != nil {
				bucketReq.PartitionId = proto.Int64(*s.partitionID)
			}
			if s.limit != nil {
				bucketReq.Limit = proto.Int64(*s.limit)
			}
			req.BucketScanReq = bucketReq
		}
		req.CallSeqId = proto.Int32(s.callSeqID)
		if s.batchSizeBytes > 0 {
			req.BatchSizeBytes = proto.Int32(s.batchSizeBytes)
		}
		return nil
	})
	if err != nil {
		return ScanKVResult{}, err
	}
	s.started = true
	s.callSeqID++
	r, ok := resp.(*flusspb.ScanKvResponse)
	if !ok {
		return ScanKVResult{}, fmt.Errorf("fluss: unexpected scan kv response type %T", resp)
	}
	if r.GetErrorCode() != 0 {
		return ScanKVResult{}, &protocol.APIError{Code: r.GetErrorCode(), Message: r.GetErrorMessage()}
	}
	result := ScanKVResult{}
	if scannerID := r.GetScannerId(); len(scannerID) > 0 {
		result.ScannerID = append([]byte(nil), scannerID...)
		s.scannerID = append([]byte(nil), result.ScannerID...)
	}
	result.HasMoreResults = r.GetHasMoreResults()
	s.hasMore = result.HasMoreResults
	result.Records = append([]byte(nil), r.GetRecords()...)
	if r.LogOffset != nil {
		v := r.GetLogOffset()
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
	_, err = s.table.client.rpc.Invoke(ctx, node.Address(), protocol.ScanKV, "ScanKvRequest", "ScanKvResponse", func(m proto.Message) error {
		req, ok := m.(*flusspb.ScanKvRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected scan kv close request type %T", m)
		}
		req.ScannerId = append([]byte(nil), s.scannerID...)
		req.CloseScanner = proto.Bool(true)
		return nil
	})
	return err
}
