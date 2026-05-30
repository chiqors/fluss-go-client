package client

import (
	"fmt"
	"math/big"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

type TypeKind = rowcodec.TypeKind

const (
	TypeBool         = rowcodec.TypeBool
	TypeInt8         = rowcodec.TypeInt8
	TypeInt16        = rowcodec.TypeInt16
	TypeInt32        = rowcodec.TypeInt32
	TypeInt64        = rowcodec.TypeInt64
	TypeFloat32      = rowcodec.TypeFloat32
	TypeFloat64      = rowcodec.TypeFloat64
	TypeString       = rowcodec.TypeString
	TypeBytes        = rowcodec.TypeBytes
	TypeDecimal      = rowcodec.TypeDecimal
	TypeDate         = rowcodec.TypeDate
	TypeTime         = rowcodec.TypeTime
	TypeTimestampNtz = rowcodec.TypeTimestampNtz
	TypeTimestampLtz = rowcodec.TypeTimestampLtz
	TypeArray        = rowcodec.TypeArray
	TypeMap          = rowcodec.TypeMap
	TypeRow          = rowcodec.TypeRow
)

type FieldType = rowcodec.FieldType
type Schema = rowcodec.Schema
type Row = rowcodec.Row
type Decimal = rowcodec.Decimal
type TimestampNtz = rowcodec.TimestampNtz
type TimestampLtz = rowcodec.TimestampLtz

func BoolType() FieldType                              { return rowcodec.BoolType() }
func Int8Type() FieldType                              { return rowcodec.Int8Type() }
func Int16Type() FieldType                             { return rowcodec.Int16Type() }
func Int32Type() FieldType                             { return rowcodec.Int32Type() }
func Int64Type() FieldType                             { return rowcodec.Int64Type() }
func Float32Type() FieldType                           { return rowcodec.Float32Type() }
func Float64Type() FieldType                           { return rowcodec.Float64Type() }
func StringType() FieldType                            { return rowcodec.StringType() }
func BytesType() FieldType                             { return rowcodec.BytesType() }
func DecimalType(precision, scale int) FieldType       { return rowcodec.DecimalType(precision, scale) }
func DateType() FieldType                              { return rowcodec.DateType() }
func TimeType() FieldType                              { return rowcodec.TimeType() }
func TimestampNtzType(precision int) FieldType         { return rowcodec.TimestampNtzType(precision) }
func TimestampLtzType(precision int) FieldType         { return rowcodec.TimestampLtzType(precision) }
func ArrayType(element FieldType) FieldType            { return rowcodec.ArrayType(element) }
func MapType(key, value FieldType) FieldType           { return rowcodec.MapType(key, value) }
func RowType(fields ...FieldType) FieldType            { return rowcodec.RowType(fields...) }
func NewSchema(fields ...FieldType) Schema             { return rowcodec.NewSchema(fields...) }
func NewRow(schema Schema, values ...any) (Row, error) { return rowcodec.NewRow(schema, values...) }

func NewDecimalFromString(value string, precision, scale int) (Decimal, error) {
	field := rowcodec.DecimalType(precision, scale)
	if err := field.Validate(); err != nil {
		return Decimal{}, err
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok {
		return Decimal{}, fmt.Errorf("fluss: invalid decimal string %q", value)
	}
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat.Mul(rat, new(big.Rat).SetInt(scaleFactor))
	if !rat.IsInt() {
		return Decimal{}, fmt.Errorf("fluss: decimal %q does not fit scale %d", value, scale)
	}
	return Decimal{Unscaled: new(big.Int).Set(rat.Num()), Scale: scale}, nil
}
