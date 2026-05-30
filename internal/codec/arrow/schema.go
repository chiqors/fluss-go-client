package arrowcodec

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/chiqors/fluss-go-client/internal/codec/row"
)

// SchemaFromRowSchema converts the internal row schema into an Apache Arrow schema.
func SchemaFromRowSchema(schema rowcodec.Schema) (*arrow.Schema, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, 0, len(schema.Fields))
	for i, field := range schema.Fields {
		dataType, err := dataTypeFromFieldType(field)
		if err != nil {
			return nil, fmt.Errorf("arrowcodec: field %d: %w", i, err)
		}
		fields = append(fields, arrow.Field{
			Name:     fmt.Sprintf("f%d", i),
			Type:     dataType,
			Nullable: true,
		})
	}
	return arrow.NewSchema(fields, nil), nil
}

func dataTypeFromFieldType(field rowcodec.FieldType) (arrow.DataType, error) {
	switch field.Kind {
	case rowcodec.TypeBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case rowcodec.TypeInt8:
		return arrow.PrimitiveTypes.Int8, nil
	case rowcodec.TypeInt16:
		return arrow.PrimitiveTypes.Int16, nil
	case rowcodec.TypeInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case rowcodec.TypeInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case rowcodec.TypeFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case rowcodec.TypeFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case rowcodec.TypeString:
		return arrow.BinaryTypes.String, nil
	case rowcodec.TypeBytes:
		return arrow.BinaryTypes.Binary, nil
	case rowcodec.TypeDecimal:
		return &arrow.Decimal128Type{
			Precision: int32(field.Length),
			Scale:     int32(field.Scale),
		}, nil
	case rowcodec.TypeDate:
		return arrow.FixedWidthTypes.Date32, nil
	case rowcodec.TypeTime:
		return arrow.FixedWidthTypes.Time32ms, nil
	case rowcodec.TypeTimestampNtz:
		return timestampTypeForPrecision(field.Length), nil
	case rowcodec.TypeTimestampLtz:
		dt := timestampTypeForPrecision(field.Length)
		switch ts := dt.(type) {
		case *arrow.TimestampType:
			return &arrow.TimestampType{Unit: ts.Unit, TimeZone: "UTC"}, nil
		default:
			return nil, fmt.Errorf("arrowcodec: unexpected timestamp type %T", dt)
		}
	case rowcodec.TypeArray:
		if field.Element == nil {
			return nil, fmt.Errorf("arrowcodec: array element type is required")
		}
		elementType, err := dataTypeFromFieldType(*field.Element)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elementType), nil
	case rowcodec.TypeMap:
		if field.Key == nil || field.Value == nil {
			return nil, fmt.Errorf("arrowcodec: map key/value types are required")
		}
		keyType, err := dataTypeFromFieldType(*field.Key)
		if err != nil {
			return nil, err
		}
		valueType, err := dataTypeFromFieldType(*field.Value)
		if err != nil {
			return nil, err
		}
		return arrow.MapOf(keyType, valueType), nil
	case rowcodec.TypeRow:
		fields := make([]arrow.Field, 0, len(field.RowFields))
		for i, rowField := range field.RowFields {
			dataType, err := dataTypeFromFieldType(rowField)
			if err != nil {
				return nil, fmt.Errorf("arrowcodec: row field %d: %w", i, err)
			}
			fields = append(fields, arrow.Field{
				Name:     fmt.Sprintf("f%d", i),
				Type:     dataType,
				Nullable: true,
			})
		}
		return arrow.StructOf(fields...), nil
	default:
		return nil, fmt.Errorf("arrowcodec: unsupported type %q", field.Kind)
	}
}

func timestampTypeForPrecision(precision int) arrow.DataType {
	switch {
	case precision <= 0:
		return &arrow.TimestampType{Unit: arrow.Second}
	case precision <= 3:
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case precision <= 6:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	default:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	}
}
