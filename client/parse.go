package client

import (
	"fmt"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func parseProduceResults(msg proto.Message) ([]ProduceResult, error) {
	resp, ok := msg.(*flusspb.ProduceLogResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected produce response type %T", msg)
	}
	out := make([]ProduceResult, 0, len(resp.GetBucketsResp()))
	failures := make([]BucketWriteError, 0)
	for _, item := range resp.GetBucketsResp() {
		result := ProduceResult{
			BucketID:   item.GetBucketId(),
			BaseOffset: item.GetBaseOffset(),
		}
		if item.PartitionId != nil {
			pid := item.GetPartitionId()
			result.PartitionID = &pid
		}
		if item.ErrorCode != nil {
			code := item.GetErrorCode()
			result.ErrorCode = &code
		}
		if item.ErrorMessage != nil {
			msg := item.GetErrorMessage()
			result.ErrorMessage = &msg
		}
		if err := bucketAPIError(item.GetErrorCode(), item.GetErrorMessage()); err != nil {
			failures = append(failures, BucketWriteError{
				PartitionID: result.PartitionID,
				BucketID:    result.BucketID,
				Err:         err,
			})
		}
		out = append(out, result)
	}
	if len(failures) > 0 {
		return out, &PartialWriteError{Operation: "append log", Failures: failures}
	}
	return out, nil
}

func parsePutResults(msg proto.Message) ([]PutResult, error) {
	resp, ok := msg.(*flusspb.PutKvResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected put response type %T", msg)
	}
	out := make([]PutResult, 0, len(resp.GetBucketsResp()))
	failures := make([]BucketWriteError, 0)
	for _, item := range resp.GetBucketsResp() {
		result := PutResult{
			BucketID:     item.GetBucketId(),
			LogEndOffset: item.GetLogEndOffset(),
		}
		if item.PartitionId != nil {
			pid := item.GetPartitionId()
			result.PartitionID = &pid
		}
		if item.ErrorCode != nil {
			code := item.GetErrorCode()
			result.ErrorCode = &code
		}
		if item.ErrorMessage != nil {
			msg := item.GetErrorMessage()
			result.ErrorMessage = &msg
		}
		if err := bucketAPIError(item.GetErrorCode(), item.GetErrorMessage()); err != nil {
			failures = append(failures, BucketWriteError{
				PartitionID: result.PartitionID,
				BucketID:    result.BucketID,
				Err:         err,
			})
		}
		out = append(out, result)
	}
	if len(failures) > 0 {
		return out, &PartialWriteError{Operation: "upsert kv", Failures: failures}
	}
	return out, nil
}

func parseLookupResults(msg proto.Message) ([]LookupBucketValues, error) {
	resp, ok := msg.(*flusspb.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected lookup response type %T", msg)
	}
	out := make([]LookupBucketValues, 0, len(resp.GetBucketsResp()))
	for _, item := range resp.GetBucketsResp() {
		if err := bucketAPIError(item.GetErrorCode(), item.GetErrorMessage()); err != nil {
			return nil, err
		}
		result := LookupBucketValues{BucketID: item.GetBucketId()}
		if item.PartitionId != nil {
			pid := item.GetPartitionId()
			result.PartitionID = &pid
		}
		for _, value := range item.GetValues() {
			if value.Values != nil {
				result.Values = append(result.Values, append([]byte(nil), value.GetValues()...))
				continue
			}
			result.Values = append(result.Values, nil)
		}
		out = append(out, result)
	}
	return out, nil
}

func parsePrefixLookupResults(msg proto.Message) ([]PrefixLookupBucketValues, error) {
	resp, ok := msg.(*flusspb.PrefixLookupResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected prefix lookup response type %T", msg)
	}
	out := make([]PrefixLookupBucketValues, 0, len(resp.GetBucketsResp()))
	for _, item := range resp.GetBucketsResp() {
		if err := bucketAPIError(item.GetErrorCode(), item.GetErrorMessage()); err != nil {
			return nil, err
		}
		result := PrefixLookupBucketValues{BucketID: item.GetBucketId()}
		if item.PartitionId != nil {
			pid := item.GetPartitionId()
			result.PartitionID = &pid
		}
		for _, valueList := range item.GetValueLists() {
			entry := make([][]byte, 0, len(valueList.GetValues()))
			for _, value := range valueList.GetValues() {
				entry = append(entry, append([]byte(nil), value...))
			}
			result.Values = append(result.Values, entry)
		}
		out = append(out, result)
	}
	return out, nil
}

func parseFetchResults(msg proto.Message) ([]FetchedBucket, error) {
	resp, ok := msg.(*flusspb.FetchLogResponse)
	if !ok {
		return nil, fmt.Errorf("fluss: unexpected fetch response type %T", msg)
	}
	var out []FetchedBucket
	for _, table := range resp.GetTablesResp() {
		for _, item := range table.GetBucketsResp() {
			if err := bucketAPIError(item.GetErrorCode(), item.GetErrorMessage()); err != nil {
				return nil, err
			}
			result := FetchedBucket{
				BucketID:       item.GetBucketId(),
				HighWatermark:  item.GetHighWatermark(),
				LogStartOffset: item.GetLogStartOffset(),
				Records:        append([]byte(nil), item.GetRecords()...),
			}
			if item.PartitionId != nil {
				pid := item.GetPartitionId()
				result.PartitionID = &pid
			}
			if item.FilteredEndOffset != nil {
				v := item.GetFilteredEndOffset()
				result.FilteredEndOffset = &v
			}
			out = append(out, result)
		}
	}
	return out, nil
}

func bucketAPIError(code int32, message string) error {
	if code == 0 {
		return nil
	}
	return &protocol.APIError{Code: code, Message: message}
}
