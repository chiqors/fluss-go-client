package metadata

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

type SchemaColumn struct {
	Name string
	Type rowcodec.FieldType
}

type schemaDescriptorJSON struct {
	Columns []schemaColumnJSON `json:"columns"`
}

type schemaColumnJSON struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	DataType  *dataTypeJSON `json:"data_type"`
	FieldType *dataTypeJSON `json:"field_type"`
}

type dataTypeJSON struct {
	Type      string            `json:"type"`
	Nullable  *bool             `json:"nullable"`
	Length    *int              `json:"length"`
	Precision *int              `json:"precision"`
	Scale     *int              `json:"scale"`
	Element   *dataTypeJSON     `json:"element_type"`
	Key       *dataTypeJSON     `json:"key_type"`
	Value     *dataTypeJSON     `json:"value_type"`
	Fields    []nestedFieldJSON `json:"fields"`
}

type nestedFieldJSON struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	DataType  *dataTypeJSON `json:"data_type"`
	FieldType *dataTypeJSON `json:"field_type"`
}

func ParseSchemaColumns(schemaJSON []byte) ([]SchemaColumn, error) {
	var desc schemaDescriptorJSON
	if err := json.Unmarshal(schemaJSON, &desc); err != nil {
		return nil, err
	}
	columns := make([]SchemaColumn, 0, len(desc.Columns))
	for _, column := range desc.Columns {
		fieldType, err := column.fieldType()
		if err != nil {
			typeLabel := column.typeLabel()
			return nil, fmt.Errorf("parse schema column %q type %q: %w", column.Name, typeLabel, err)
		}
		columns = append(columns, SchemaColumn{
			Name: column.Name,
			Type: fieldType,
		})
	}
	return columns, nil
}

func ParseSchema(schemaJSON []byte) (rowcodec.Schema, []string, error) {
	columns, err := ParseSchemaColumns(schemaJSON)
	if err != nil {
		return rowcodec.Schema{}, nil, err
	}
	fields := make([]rowcodec.FieldType, 0, len(columns))
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
		fields = append(fields, column.Type)
	}
	return rowcodec.NewSchema(fields...), names, nil
}

func (c schemaColumnJSON) fieldType() (rowcodec.FieldType, error) {
	if c.DataType != nil {
		return c.DataType.toFieldType()
	}
	if c.FieldType != nil {
		return c.FieldType.toFieldType()
	}
	if strings.TrimSpace(c.Type) != "" {
		return parseFieldType(c.Type)
	}
	return rowcodec.FieldType{}, fmt.Errorf("unsupported type %q", "")
}

func (c schemaColumnJSON) typeLabel() string {
	if strings.TrimSpace(c.Type) != "" {
		return c.Type
	}
	if c.DataType != nil {
		return c.DataType.label()
	}
	if c.FieldType != nil {
		return c.FieldType.label()
	}
	return ""
}

func (f nestedFieldJSON) fieldType() (rowcodec.FieldType, error) {
	if f.FieldType != nil {
		return f.FieldType.toFieldType()
	}
	if f.DataType != nil {
		return f.DataType.toFieldType()
	}
	if strings.TrimSpace(f.Type) != "" {
		return parseFieldType(f.Type)
	}
	return rowcodec.FieldType{}, fmt.Errorf("unsupported type %q", "")
}

func (d *dataTypeJSON) label() string {
	if d == nil {
		return ""
	}
	return d.Type
}

func (d *dataTypeJSON) toFieldType() (rowcodec.FieldType, error) {
	if d == nil {
		return rowcodec.FieldType{}, fmt.Errorf("missing data type")
	}
	switch strings.ToUpper(strings.TrimSpace(d.Type)) {
	case "BOOLEAN":
		return rowcodec.BoolType(), nil
	case "TINYINT":
		return rowcodec.Int8Type(), nil
	case "SMALLINT":
		return rowcodec.Int16Type(), nil
	case "INT", "INTEGER":
		return rowcodec.Int32Type(), nil
	case "BIGINT":
		return rowcodec.Int64Type(), nil
	case "FLOAT":
		return rowcodec.Float32Type(), nil
	case "DOUBLE":
		return rowcodec.Float64Type(), nil
	case "STRING", "VARCHAR", "CHAR":
		return rowcodec.StringType(), nil
	case "BYTES", "BINARY", "VARBINARY":
		return rowcodec.BytesType(), nil
	case "DECIMAL":
		precision := valueOrDefault(d.Precision, 0)
		scale := valueOrDefault(d.Scale, 0)
		return rowcodec.DecimalType(precision, scale), nil
	case "DATE":
		return rowcodec.DateType(), nil
	case "TIME", "TIME_WITHOUT_TIME_ZONE":
		return rowcodec.TimeType(), nil
	case "TIMESTAMP", "TIMESTAMP_WITHOUT_TIME_ZONE":
		precision := valueOrDefault(d.Precision, 6)
		return rowcodec.TimestampNtzType(precision), nil
	case "TIMESTAMP_LTZ", "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
		precision := valueOrDefault(d.Precision, 6)
		return rowcodec.TimestampLtzType(precision), nil
	case "ARRAY":
		if d.Element == nil {
			return rowcodec.FieldType{}, fmt.Errorf("missing array element type")
		}
		element, err := d.Element.toFieldType()
		if err != nil {
			return rowcodec.FieldType{}, fmt.Errorf("array element type: %w", err)
		}
		return rowcodec.ArrayType(element), nil
	case "MAP":
		if d.Key == nil || d.Value == nil {
			return rowcodec.FieldType{}, fmt.Errorf("missing map key/value type")
		}
		key, err := d.Key.toFieldType()
		if err != nil {
			return rowcodec.FieldType{}, fmt.Errorf("map key type: %w", err)
		}
		value, err := d.Value.toFieldType()
		if err != nil {
			return rowcodec.FieldType{}, fmt.Errorf("map value type: %w", err)
		}
		return rowcodec.MapType(key, value), nil
	case "ROW":
		fields := make([]rowcodec.FieldType, 0, len(d.Fields))
		for i, field := range d.Fields {
			fieldType, err := field.fieldType()
			if err != nil {
				return rowcodec.FieldType{}, fmt.Errorf("row field %d %q: %w", i, field.Name, err)
			}
			fields = append(fields, fieldType)
		}
		return rowcodec.RowType(fields...), nil
	default:
		if strings.TrimSpace(d.Type) == "" {
			return rowcodec.FieldType{}, fmt.Errorf("unsupported type %q", "")
		}
		return rowcodec.FieldType{}, fmt.Errorf("unsupported type %q", d.Type)
	}
}

func valueOrDefault(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

type fieldTypeParser struct {
	input string
	pos   int
}

func parseFieldType(input string) (rowcodec.FieldType, error) {
	p := &fieldTypeParser{input: strings.TrimSpace(input)}
	field, err := p.parseType()
	if err != nil {
		return rowcodec.FieldType{}, err
	}
	p.skipSpaces()
	if p.pos != len(p.input) {
		return rowcodec.FieldType{}, fmt.Errorf("unexpected trailing input %q", p.input[p.pos:])
	}
	return field, nil
}

func (p *fieldTypeParser) parseType() (rowcodec.FieldType, error) {
	p.skipSpaces()
	ident := strings.ToUpper(p.parseIdentifier())
	switch ident {
	case "BOOLEAN":
		return rowcodec.BoolType(), nil
	case "TINYINT":
		return rowcodec.Int8Type(), nil
	case "SMALLINT":
		return rowcodec.Int16Type(), nil
	case "INT", "INTEGER":
		return rowcodec.Int32Type(), nil
	case "BIGINT":
		return rowcodec.Int64Type(), nil
	case "FLOAT":
		return rowcodec.Float32Type(), nil
	case "DOUBLE":
		return rowcodec.Float64Type(), nil
	case "STRING", "VARCHAR", "CHAR":
		if p.peek() == '(' {
			if _, err := p.parseIntArgs(1); err != nil {
				return rowcodec.FieldType{}, err
			}
		}
		return rowcodec.StringType(), nil
	case "BYTES", "BINARY", "VARBINARY":
		if p.peek() == '(' {
			if _, err := p.parseIntArgs(1); err != nil {
				return rowcodec.FieldType{}, err
			}
		}
		return rowcodec.BytesType(), nil
	case "DECIMAL":
		args, err := p.parseIntArgs(2)
		if err != nil {
			return rowcodec.FieldType{}, err
		}
		return rowcodec.DecimalType(args[0], args[1]), nil
	case "DATE":
		return rowcodec.DateType(), nil
	case "TIME":
		if p.peek() == '(' {
			if _, err := p.parseIntArgs(1); err != nil {
				return rowcodec.FieldType{}, err
			}
		}
		return rowcodec.TimeType(), nil
	case "TIMESTAMP":
		precision := 6
		if p.peek() == '(' {
			args, err := p.parseIntArgs(1)
			if err != nil {
				return rowcodec.FieldType{}, err
			}
			precision = args[0]
		}
		return rowcodec.TimestampNtzType(precision), nil
	case "TIMESTAMP_LTZ":
		precision := 6
		if p.peek() == '(' {
			args, err := p.parseIntArgs(1)
			if err != nil {
				return rowcodec.FieldType{}, err
			}
			precision = args[0]
		}
		return rowcodec.TimestampLtzType(precision), nil
	case "ARRAY":
		if err := p.expect('<'); err != nil {
			return rowcodec.FieldType{}, err
		}
		element, err := p.parseType()
		if err != nil {
			return rowcodec.FieldType{}, err
		}
		if err := p.expect('>'); err != nil {
			return rowcodec.FieldType{}, err
		}
		return rowcodec.ArrayType(element), nil
	case "MAP":
		if err := p.expect('<'); err != nil {
			return rowcodec.FieldType{}, err
		}
		keyType, err := p.parseType()
		if err != nil {
			return rowcodec.FieldType{}, err
		}
		if err := p.expect(','); err != nil {
			return rowcodec.FieldType{}, err
		}
		valueType, err := p.parseType()
		if err != nil {
			return rowcodec.FieldType{}, err
		}
		if err := p.expect('>'); err != nil {
			return rowcodec.FieldType{}, err
		}
		return rowcodec.MapType(keyType, valueType), nil
	case "ROW":
		if err := p.expect('<'); err != nil {
			return rowcodec.FieldType{}, err
		}
		fields := make([]rowcodec.FieldType, 0)
		for {
			p.skipSpaces()
			_ = p.parseIdentifier()
			p.skipSpaces()
			fieldType, err := p.parseType()
			if err != nil {
				return rowcodec.FieldType{}, err
			}
			fields = append(fields, fieldType)
			p.skipSpaces()
			if p.peek() == '>' {
				break
			}
			if err := p.expect(','); err != nil {
				return rowcodec.FieldType{}, err
			}
		}
		if err := p.expect('>'); err != nil {
			return rowcodec.FieldType{}, err
		}
		return rowcodec.RowType(fields...), nil
	default:
		return rowcodec.FieldType{}, fmt.Errorf("unsupported type %q", ident)
	}
}

func (p *fieldTypeParser) parseIntArgs(expect int) ([]int, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}
	args := make([]int, 0, expect)
	for {
		p.skipSpaces()
		start := p.pos
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
			p.pos++
		}
		if start == p.pos {
			return nil, fmt.Errorf("expected integer argument")
		}
		value, err := strconv.Atoi(p.input[start:p.pos])
		if err != nil {
			return nil, err
		}
		args = append(args, value)
		p.skipSpaces()
		if p.peek() == ')' {
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}
	if err := p.expect(')'); err != nil {
		return nil, err
	}
	if len(args) != expect {
		return nil, fmt.Errorf("expected %d integer arguments, got %d", expect, len(args))
	}
	return args, nil
}

func (p *fieldTypeParser) parseIdentifier() string {
	p.skipSpaces()
	start := p.pos
	for p.pos < len(p.input) {
		r := rune(p.input[p.pos])
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			p.pos++
			continue
		}
		break
	}
	return p.input[start:p.pos]
}

func (p *fieldTypeParser) skipSpaces() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *fieldTypeParser) expect(ch byte) error {
	p.skipSpaces()
	if p.pos >= len(p.input) || p.input[p.pos] != ch {
		return fmt.Errorf("expected %q", ch)
	}
	p.pos++
	return nil
}

func (p *fieldTypeParser) peek() byte {
	p.skipSpaces()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}
