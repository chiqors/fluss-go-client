package rowcodec

import "fmt"

type TypeKind string

const (
	TypeBool    TypeKind = "bool"
	TypeInt32   TypeKind = "int32"
	TypeInt64   TypeKind = "int64"
	TypeFloat64 TypeKind = "float64"
	TypeString  TypeKind = "string"
	TypeBytes   TypeKind = "bytes"
	TypeDecimal TypeKind = "decimal"
)

type FieldType struct {
	Kind   TypeKind
	Length int
	Scale  int
}

func BoolType() FieldType { return FieldType{Kind: TypeBool} }

func Int32Type() FieldType { return FieldType{Kind: TypeInt32} }

func Int64Type() FieldType { return FieldType{Kind: TypeInt64} }

func Float64Type() FieldType { return FieldType{Kind: TypeFloat64} }

func StringType() FieldType { return FieldType{Kind: TypeString} }

func BytesType() FieldType { return FieldType{Kind: TypeBytes} }

func DecimalType(precision, scale int) FieldType {
	return FieldType{Kind: TypeDecimal, Length: precision, Scale: scale}
}

func (t FieldType) IsFixed() bool {
	switch t.Kind {
	case TypeBool, TypeInt32, TypeInt64, TypeFloat64:
		return true
	default:
		return false
	}
}

func (t FieldType) Validate() error {
	switch t.Kind {
	case TypeBool, TypeInt32, TypeInt64, TypeFloat64, TypeString, TypeBytes:
		return nil
	case TypeDecimal:
		if t.Length <= 0 {
			return fmt.Errorf("rowcodec: decimal precision must be positive")
		}
		if t.Scale < 0 || t.Scale > t.Length {
			return fmt.Errorf("rowcodec: invalid decimal scale")
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
