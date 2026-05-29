package client

import (
	"github.com/chiqors/fluss-client-go/internal/pbutil"
	"github.com/chiqors/fluss-client-go/protocol"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func parseProduceResults(msg protoreflect.Message) ([]ProduceResult, error) {
	field, _ := pbutil.Field(msg.Descriptor(), "buckets_resp")
	list := msg.Get(field).List()
	out := make([]ProduceResult, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		if err := bucketError(item); err != nil {
			return nil, err
		}
		bucketField, _ := pbutil.Field(item.Descriptor(), "bucket_id")
		baseField, _ := pbutil.Field(item.Descriptor(), "base_offset")
		result := ProduceResult{
			BucketID:   int32(item.Get(bucketField).Int()),
			BaseOffset: item.Get(baseField).Int(),
		}
		if pidField := item.Descriptor().Fields().ByName("partition_id"); pidField != nil && item.Has(pidField) {
			pid := item.Get(pidField).Int()
			result.PartitionID = &pid
		}
		out = append(out, result)
	}
	return out, nil
}

func parsePutResults(msg protoreflect.Message) ([]PutResult, error) {
	field, _ := pbutil.Field(msg.Descriptor(), "buckets_resp")
	list := msg.Get(field).List()
	out := make([]PutResult, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		if err := bucketError(item); err != nil {
			return nil, err
		}
		bucketField, _ := pbutil.Field(item.Descriptor(), "bucket_id")
		offsetField, _ := pbutil.Field(item.Descriptor(), "log_end_offset")
		result := PutResult{
			BucketID:     int32(item.Get(bucketField).Int()),
			LogEndOffset: item.Get(offsetField).Int(),
		}
		if pidField := item.Descriptor().Fields().ByName("partition_id"); pidField != nil && item.Has(pidField) {
			pid := item.Get(pidField).Int()
			result.PartitionID = &pid
		}
		out = append(out, result)
	}
	return out, nil
}

func parseLookupResults(msg protoreflect.Message) ([]LookupBucketValues, error) {
	field, _ := pbutil.Field(msg.Descriptor(), "buckets_resp")
	list := msg.Get(field).List()
	out := make([]LookupBucketValues, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		if err := bucketError(item); err != nil {
			return nil, err
		}
		bucketField, _ := pbutil.Field(item.Descriptor(), "bucket_id")
		result := LookupBucketValues{BucketID: int32(item.Get(bucketField).Int())}
		if pidField := item.Descriptor().Fields().ByName("partition_id"); pidField != nil && item.Has(pidField) {
			pid := item.Get(pidField).Int()
			result.PartitionID = &pid
		}
		valuesField, _ := pbutil.Field(item.Descriptor(), "values")
		values := item.Get(valuesField).List()
		for j := 0; j < values.Len(); j++ {
			valueMsg := values.Get(j).Message()
			valueField, _ := pbutil.Field(valueMsg.Descriptor(), "values")
			if valueMsg.Has(valueField) {
				result.Values = append(result.Values, append([]byte(nil), valueMsg.Get(valueField).Bytes()...))
			} else {
				result.Values = append(result.Values, nil)
			}
		}
		out = append(out, result)
	}
	return out, nil
}

func parsePrefixLookupResults(msg protoreflect.Message) ([]PrefixLookupBucketValues, error) {
	field, _ := pbutil.Field(msg.Descriptor(), "buckets_resp")
	list := msg.Get(field).List()
	out := make([]PrefixLookupBucketValues, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		if err := bucketError(item); err != nil {
			return nil, err
		}
		bucketField, _ := pbutil.Field(item.Descriptor(), "bucket_id")
		result := PrefixLookupBucketValues{BucketID: int32(item.Get(bucketField).Int())}
		if pidField := item.Descriptor().Fields().ByName("partition_id"); pidField != nil && item.Has(pidField) {
			pid := item.Get(pidField).Int()
			result.PartitionID = &pid
		}
		listsField, _ := pbutil.Field(item.Descriptor(), "value_lists")
		lists := item.Get(listsField).List()
		for j := 0; j < lists.Len(); j++ {
			valueListMsg := lists.Get(j).Message()
			valuesField, _ := pbutil.Field(valueListMsg.Descriptor(), "values")
			valueList := valueListMsg.Get(valuesField).List()
			entry := make([][]byte, 0, valueList.Len())
			for k := 0; k < valueList.Len(); k++ {
				entry = append(entry, append([]byte(nil), valueList.Get(k).Bytes()...))
			}
			result.Values = append(result.Values, entry)
		}
		out = append(out, result)
	}
	return out, nil
}

func parseFetchResults(msg protoreflect.Message) ([]FetchedBucket, error) {
	tablesField, _ := pbutil.Field(msg.Descriptor(), "tables_resp")
	tables := msg.Get(tablesField).List()
	var out []FetchedBucket
	for i := 0; i < tables.Len(); i++ {
		tableMsg := tables.Get(i).Message()
		bucketsField, _ := pbutil.Field(tableMsg.Descriptor(), "buckets_resp")
		buckets := tableMsg.Get(bucketsField).List()
		for j := 0; j < buckets.Len(); j++ {
			item := buckets.Get(j).Message()
			if err := bucketError(item); err != nil {
				return nil, err
			}
			bucketField, _ := pbutil.Field(item.Descriptor(), "bucket_id")
			hwmField, _ := pbutil.Field(item.Descriptor(), "high_watermark")
			startField, _ := pbutil.Field(item.Descriptor(), "log_start_offset")
			recordField, _ := pbutil.Field(item.Descriptor(), "records")
			result := FetchedBucket{
				BucketID:       int32(item.Get(bucketField).Int()),
				HighWatermark:  item.Get(hwmField).Int(),
				LogStartOffset: item.Get(startField).Int(),
				Records:        append([]byte(nil), item.Get(recordField).Bytes()...),
			}
			if pidField := item.Descriptor().Fields().ByName("partition_id"); pidField != nil && item.Has(pidField) {
				pid := item.Get(pidField).Int()
				result.PartitionID = &pid
			}
			if endField := item.Descriptor().Fields().ByName("filtered_end_offset"); endField != nil && item.Has(endField) {
				v := item.Get(endField).Int()
				result.FilteredEndOffset = &v
			}
			out = append(out, result)
		}
	}
	return out, nil
}

func bucketError(msg protoreflect.Message) error {
	codeField := msg.Descriptor().Fields().ByName("error_code")
	if codeField == nil || !msg.Has(codeField) {
		return nil
	}
	code := int32(msg.Get(codeField).Int())
	if code == 0 {
		return nil
	}
	apiErr := &protocol.APIError{Code: code}
	if messageField := msg.Descriptor().Fields().ByName("error_message"); messageField != nil && msg.Has(messageField) {
		apiErr.Message = msg.Get(messageField).String()
	}
	return apiErr
}
