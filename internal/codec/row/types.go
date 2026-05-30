package rowcodec

import (
	"fmt"
	"math/big"
)

type TypeKind string

const (
	TypeBool         TypeKind = "bool"
	TypeInt8         TypeKind = "int8"
	TypeInt16        TypeKind = "int16"
	TypeInt32        TypeKind = "int32"
	TypeInt64        TypeKind = "int64"
	TypeFloat32      TypeKind = "float32"
	TypeFloat64      TypeKind = "float64"
	TypeString       TypeKind = "string"
	TypeBytes        TypeKind = "bytes"
	TypeDecimal      TypeKind = "decimal"
	TypeDate         TypeKind = "date"
	TypeTime         TypeKind = "time"
	TypeTimestampNtz TypeKind = "timestamp_ntz"
	TypeTimestampLtz TypeKind = "timestamp_ltz"
	TypeArray        TypeKind = "array"
	TypeMap          TypeKind = "map"
	TypeRow          TypeKind = "row"
)

type FieldType struct {
	Kind      TypeKind
	Length    int
	Scale     int
	Element   *FieldType
	Key       *FieldType
	Value     *FieldType
	RowFields []FieldType
}

func BoolType() FieldType { return FieldType{Kind: TypeBool} }

func Int8Type() FieldType { return FieldType{Kind: TypeInt8} }

func Int16Type() FieldType { return FieldType{Kind: TypeInt16} }

func Int32Type() FieldType { return FieldType{Kind: TypeInt32} }

func Int64Type() FieldType { return FieldType{Kind: TypeInt64} }

func Float32Type() FieldType { return FieldType{Kind: TypeFloat32} }

func Float64Type() FieldType { return FieldType{Kind: TypeFloat64} }

func StringType() FieldType { return FieldType{Kind: TypeString} }

func BytesType() FieldType { return FieldType{Kind: TypeBytes} }

func DecimalType(precision, scale int) FieldType {
	return FieldType{Kind: TypeDecimal, Length: precision, Scale: scale}
}

func DateType() FieldType { return FieldType{Kind: TypeDate} }

func TimeType() FieldType { return FieldType{Kind: TypeTime} }

func TimestampNtzType(precision int) FieldType {
	return FieldType{Kind: TypeTimestampNtz, Length: precision}
}

func TimestampLtzType(precision int) FieldType {
	return FieldType{Kind: TypeTimestampLtz, Length: precision}
}

func ArrayType(element FieldType) FieldType {
	return FieldType{Kind: TypeArray, Element: &element}
}

func MapType(key, value FieldType) FieldType {
	return FieldType{Kind: TypeMap, Key: &key, Value: &value}
}

func RowType(fields ...FieldType) FieldType {
	return FieldType{Kind: TypeRow, RowFields: append([]FieldType(nil), fields...)}
}

type Decimal struct {
	Unscaled *big.Int
	Scale    int
}

type TimestampNtz struct {
	Millisecond       int64
	NanoOfMillisecond int32
}

type TimestampLtz struct {
	EpochMillisecond  int64
	NanoOfMillisecond int32
}

func (t FieldType) IsFixed() bool {
	switch t.Kind {
	case TypeBool, TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64, TypeDate, TypeTime:
		return true
	case TypeDecimal:
		return t.Length > 0 && t.Length <= 18
	case TypeTimestampNtz, TypeTimestampLtz:
		return t.Length >= 0 && t.Length <= 3
	default:
		return false
	}
}

func (t FieldType) Validate() error {
	switch t.Kind {
	case TypeBool, TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64, TypeString, TypeBytes, TypeDate, TypeTime:
		return nil
	case TypeDecimal:
		if t.Length <= 0 {
			return fmt.Errorf("rowcodec: decimal precision must be positive")
		}
		if t.Scale < 0 || t.Scale > t.Length {
			return fmt.Errorf("rowcodec: invalid decimal scale")
		}
		return nil
	case TypeTimestampNtz, TypeTimestampLtz:
		if t.Length < 0 || t.Length > 9 {
			return fmt.Errorf("rowcodec: invalid timestamp precision %d", t.Length)
		}
		return nil
	case TypeArray:
		if t.Element == nil {
			return fmt.Errorf("rowcodec: array element type is required")
		}
		return t.Element.Validate()
	case TypeMap:
		if t.Key == nil || t.Value == nil {
			return fmt.Errorf("rowcodec: map key and value types are required")
		}
		if err := t.Key.Validate(); err != nil {
			return err
		}
		return t.Value.Validate()
	case TypeRow:
		for i, field := range t.RowFields {
			if err := field.Validate(); err != nil {
				return fmt.Errorf("rowcodec: row field %d: %w", i, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("rowcodec: unsupported type %q", t.Kind)
	}
}

type Schema struct {
	Fields []FieldType
}

func NewSchema(fields ...FieldType) Schema {
	return Schema{Fields: append([]FieldType(nil), fields...)}
}

func (s Schema) Validate() error {
	for i, field := range s.Fields {
		if err := field.Validate(); err != nil {
			return fmt.Errorf("data: field %d: %w", i, err)
		}
	}
	return nil
}
