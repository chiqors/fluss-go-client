package arrowcodec

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

func TestSchemaFromRowSchemaScalarsAndTemporal(t *testing.T) {
	schema := rowcodec.NewSchema(
		rowcodec.BoolType(),
		rowcodec.Int8Type(),
		rowcodec.Int16Type(),
		rowcodec.Int32Type(),
		rowcodec.Int64Type(),
		rowcodec.Float32Type(),
		rowcodec.Float64Type(),
		rowcodec.StringType(),
		rowcodec.BytesType(),
		rowcodec.DecimalType(10, 2),
		rowcodec.DateType(),
		rowcodec.TimeType(),
		rowcodec.TimestampNtzType(6),
		rowcodec.TimestampLtzType(6),
	)

	got, err := SchemaFromRowSchema(schema)
	if err != nil {
		t.Fatalf("SchemaFromRowSchema() error = %v", err)
	}
	if got.NumFields() != 14 {
		t.Fatalf("field count = %d, want 14", got.NumFields())
	}

	wantTypes := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.Binary,
		&arrow.Decimal128Type{Precision: 10, Scale: 2},
		arrow.FixedWidthTypes.Date32,
		arrow.FixedWidthTypes.Time32ms,
		&arrow.TimestampType{Unit: arrow.Microsecond},
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
	}

	for i, want := range wantTypes {
		if !arrow.TypeEqual(got.Field(i).Type, want) {
			t.Fatalf("field[%d] type = %v, want %v", i, got.Field(i).Type, want)
		}
	}
}

func TestSchemaFromRowSchemaCompositeTypes(t *testing.T) {
	schema := rowcodec.NewSchema(
		rowcodec.ArrayType(rowcodec.Int32Type()),
		rowcodec.MapType(rowcodec.StringType(), rowcodec.Int64Type()),
		rowcodec.RowType(
			rowcodec.StringType(),
			rowcodec.Int32Type(),
			rowcodec.ArrayType(rowcodec.StringType()),
		),
	)

	got, err := SchemaFromRowSchema(schema)
	if err != nil {
		t.Fatalf("SchemaFromRowSchema() error = %v", err)
	}
	if got.NumFields() != 3 {
		t.Fatalf("field count = %d, want 3", got.NumFields())
	}

	if got.Field(0).Type.ID() != arrow.LIST {
		t.Fatalf("field[0] type id = %v, want LIST", got.Field(0).Type.ID())
	}
	if got.Field(1).Type.ID() != arrow.MAP {
		t.Fatalf("field[1] type id = %v, want MAP", got.Field(1).Type.ID())
	}
	if got.Field(2).Type.ID() != arrow.STRUCT {
		t.Fatalf("field[2] type id = %v, want STRUCT", got.Field(2).Type.ID())
	}
}
