package metadata

import (
	"testing"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

func TestParseSchema(t *testing.T) {
	schema, names, err := ParseSchema([]byte(`{
		"columns":[
			{"name":"id","type":"BIGINT"},
			{"name":"name","type":"STRING"},
			{"name":"tags","type":"ARRAY<STRING>"},
			{"name":"attrs","type":"MAP<STRING, BIGINT>"},
			{"name":"payload","type":"ROW<note STRING, rank_value INT>"}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseSchema() error = %v", err)
	}
	if len(names) != 5 || names[0] != "id" || names[4] != "payload" {
		t.Fatalf("unexpected names: %#v", names)
	}
	if got := schema.Fields[0]; got.Kind != rowcodec.TypeInt64 {
		t.Fatalf("unexpected id type: %#v", got)
	}
	if got := schema.Fields[2]; got.Kind != rowcodec.TypeArray || got.Element == nil || got.Element.Kind != rowcodec.TypeString {
		t.Fatalf("unexpected tags type: %#v", got)
	}
	if got := schema.Fields[3]; got.Kind != rowcodec.TypeMap || got.Key == nil || got.Value == nil || got.Key.Kind != rowcodec.TypeString || got.Value.Kind != rowcodec.TypeInt64 {
		t.Fatalf("unexpected attrs type: %#v", got)
	}
	if got := schema.Fields[4]; got.Kind != rowcodec.TypeRow || len(got.RowFields) != 2 || got.RowFields[1].Kind != rowcodec.TypeInt32 {
		t.Fatalf("unexpected payload type: %#v", got)
	}
}

func TestParseSchemaRealFlussJSON(t *testing.T) {
	schema, names, err := ParseSchema([]byte(`{
		"version":1,
		"columns":[
			{"name":"customer_id","data_type":{"type":"BIGINT","nullable":false},"id":0},
			{"name":"customer_name","data_type":{"type":"STRING"},"id":1},
			{"name":"customer_tier","data_type":{"type":"STRING"},"id":2},
			{"name":"event_ts","data_type":{"type":"TIMESTAMP_WITHOUT_TIME_ZONE","precision":6},"id":3},
			{"name":"payload","data_type":{"type":"ROW","fields":[
				{"name":"score","field_type":{"type":"INTEGER"},"field_id":4},
				{"name":"labels","field_type":{"type":"ARRAY","element_type":{"type":"STRING"}},"field_id":5}
			]},"id":4}
		],
		"primary_key":["customer_id"],
		"highest_field_id":5
	}`))
	if err != nil {
		t.Fatalf("ParseSchema() error = %v", err)
	}
	if len(names) != 5 || names[0] != "customer_id" || names[4] != "payload" {
		t.Fatalf("unexpected names: %#v", names)
	}
	if got := schema.Fields[0]; got.Kind != rowcodec.TypeInt64 {
		t.Fatalf("unexpected customer_id type: %#v", got)
	}
	if got := schema.Fields[3]; got.Kind != rowcodec.TypeTimestampNtz || got.Length != 6 {
		t.Fatalf("unexpected event_ts type: %#v", got)
	}
	if got := schema.Fields[4]; got.Kind != rowcodec.TypeRow || len(got.RowFields) != 2 {
		t.Fatalf("unexpected payload type: %#v", got)
	}
	if got := schema.Fields[4].RowFields[1]; got.Kind != rowcodec.TypeArray || got.Element == nil || got.Element.Kind != rowcodec.TypeString {
		t.Fatalf("unexpected payload.labels type: %#v", got)
	}
}
