package adaptor

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// StructToSchema converts a Go struct type to Avro schema with namespace
func StructToSchema(t reflect.Type, namespace string) (avro.Schema, error) {
	var schFields []*avro.Field
	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			// skip unexported fields
			if f.PkgPath != "" {
				continue
			}

			// Create a unique namespace for nested structs to avoid field name conflicts
			fieldNamespace := namespace
			if namespace != "" {
				fieldNamespace = namespace + "." + ToSnakeCase(f.Name)
			} else {
				fieldNamespace = ToSnakeCase(f.Name)
			}

			s, err := StructToSchema(f.Type, fieldNamespace)
			if err != nil {
				return nil, fmt.Errorf("StructToSchema: %w", err)
			}
			fName := f.Name // Use field name as avro field name
			schField, err := avro.NewField(fName, s)
			// schField, err := avro.NewField(fName, s, avro.WithDefault(avroDefaultField(s)))
			if err != nil {
				return nil, fmt.Errorf("avro.NewField: %w", err)
			}
			schFields = append(schFields, schField)
		}
		name := ToSnakeCase(t.Name())
		return avro.NewRecordSchema(name, namespace, schFields)
	case reflect.Map:
		s, err := StructToSchema(t.Elem(), namespace)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		return avro.NewMapSchema(s), nil
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
		}
		s, err := StructToSchema(t.Elem(), namespace)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		return avro.NewArraySchema(s), nil
	case reflect.Pointer:
		n := avro.NewPrimitiveSchema(avro.Null, nil)
		s, err := StructToSchema(t.Elem(), namespace)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		union, err := avro.NewUnionSchema([]avro.Schema{n, s})
		if err != nil {
			return nil, fmt.Errorf("avro.NewUnionSchema: %v, type: %s", err, s.String())
		}
		return union, nil
	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case reflect.Int, reflect.Int64:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Uint8, reflect.Uint16:
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case reflect.Uint32:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Uint64:
		name := ToSnakeCase(t.Name())
		fixedSchema, err := avro.NewFixedSchema(name, namespace, 8, avro.NewPrimitiveLogicalSchema(avro.UUID))
		if err != nil {
			return nil, fmt.Errorf("avro.NewFixedSchema: %w", err)
		}
		return fixedSchema, nil
	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		return nil, fmt.Errorf("unknown type %s", t.Kind().String())
	}
}

// MessageToSchema converts a protobuf message descriptor to Avro schema
func MessageToSchema(msg protoreflect.MessageDescriptor, namespace string) (avro.Schema, error) {
	var schFields []*avro.Field

	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)

		// Create a unique namespace for nested messages to avoid field name conflicts
		fieldNamespace := namespace
		if namespace != "" {
			fieldNamespace = namespace + "." + string(field.Name())
		} else {
			fieldNamespace = string(field.Name())
		}

		s, err := fieldToSchema(field, fieldNamespace)
		if err != nil {
			return nil, fmt.Errorf("fieldToSchema: %w", err)
		}

		// Use the protobuf field name
		fName := string(field.Name())
		schField, err := avro.NewField(fName, s, avro.WithDefault(avroDefaultField(s)))
		if err != nil {
			return nil, fmt.Errorf("avro.NewField: %w", err)
		}
		schFields = append(schFields, schField)
	}

	name := string(msg.Name())
	return avro.NewRecordSchema(name, namespace, schFields)
}

// fieldToSchema converts a protobuf field descriptor to Avro schema
func fieldToSchema(field protoreflect.FieldDescriptor, namespace string) (avro.Schema, error) {
	var schema avro.Schema
	var err error

	switch field.Kind() {
	case protoreflect.MessageKind:
		// Handle nested messages
		msgDesc := field.Message()
		if msgDesc == nil {
			return nil, fmt.Errorf("message descriptor is nil for field %s", field.Name())
		}
		schema, err = MessageToSchema(msgDesc, namespace)
		if err != nil {
			return nil, err
		}
	case protoreflect.BoolKind:
		schema = avro.NewPrimitiveSchema(avro.Boolean, nil)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		schema = avro.NewPrimitiveSchema(avro.Int, nil)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		schema = avro.NewPrimitiveSchema(avro.Long, nil)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		schema = avro.NewPrimitiveSchema(avro.Int, nil)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		schema = avro.NewPrimitiveSchema(avro.Long, nil)
	case protoreflect.FloatKind:
		schema = avro.NewPrimitiveSchema(avro.Float, nil)
	case protoreflect.DoubleKind:
		schema = avro.NewPrimitiveSchema(avro.Double, nil)
	case protoreflect.StringKind:
		schema = avro.NewPrimitiveSchema(avro.String, nil)
	case protoreflect.BytesKind:
		schema = avro.NewPrimitiveSchema(avro.Bytes, nil)
	case protoreflect.EnumKind:
		// Handle enums as strings
		schema = avro.NewPrimitiveSchema(avro.String, nil)
	default:
		return nil, fmt.Errorf("unsupported protobuf field kind: %s", field.Kind())
	}

	// Handle repeated fields (arrays)
	if field.IsList() {
		schema = avro.NewArraySchema(schema)
	}

	// Handle optional fields (nullable)
	if field.Cardinality() != protoreflect.Required {
		nullSchema := avro.NewPrimitiveSchema(avro.Null, nil)
		union, err := avro.NewUnionSchema([]avro.Schema{nullSchema, schema})
		if err != nil {
			return nil, fmt.Errorf("failed to create union schema: %w", err)
		}
		schema = union
	}

	return schema, nil
}

func avroDefaultField(s avro.Schema) any {
	switch s.Type() {
	case avro.String, avro.Bytes, avro.Enum, avro.Fixed:
		return ""
	case avro.Boolean:
		return false
	case avro.Int:
		return int(0)
	case avro.Long:
		return int64(0)
	case avro.Float:
		return float32(0.0)
	case avro.Double:
		return float64(0.0)
	case avro.Map:
		return nil
	case avro.Array:
		return nil
	default:
		return nil
	}
}
