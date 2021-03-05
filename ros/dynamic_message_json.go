package ros

// IMPORT REQUIRED PACKAGES.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"github.com/team-rocos/rosgo/libgengo"
)

// DEFINE PUBLIC STRUCTURES.

// DEFINE PRIVATE STRUCTURES.

// DEFINE PUBLIC GLOBALS.

// DEFINE PRIVATE GLOBALS.

// DEFINE PUBLIC STATIC FUNCTIONS.

// DEFINE PUBLIC RECEIVER FUNCTIONS.

//	DynamicMessageType

// GenerateJSONSchema generates a (primitive) JSON schema for the associated DynamicMessageType; however note that since
// we are mostly interested in making schema's for particular _topics_, the function takes a string prefix, and string topic name, which are
// used to id the resulting schema.
func (t *DynamicMessageType) GenerateJSONSchema(prefix string, topic string) ([]byte, error) {
	// The JSON schema for a message consist of the (recursive) properties names/types:
	schemaItems, err := t.generateJSONSchemaProperties(prefix + topic)
	if err != nil {
		return nil, err
	}

	// Plus some extra keywords:
	schemaItems["$schema"] = "https://json-schema.org/draft-07/schema#"
	schemaItems["$id"] = prefix + topic

	// The schema itself is created from the map of properties.
	schemaString, err := json.Marshal(schemaItems)
	if err != nil {
		return nil, err
	}

	// All done.
	return schemaString, nil
}

func (t *DynamicMessageType) generateJSONSchemaProperties(topic string) (map[string]interface{}, error) {
	// Each message's schema indicates that it is an 'object' with some nested properties: those properties are the fields and their types.
	properties := make(map[string]interface{})
	schemaItems := make(map[string]interface{})
	schemaItems["type"] = "object"
	schemaItems["properties"] = properties

	// Iterate over each of the fields in the message.
	for _, field := range t.spec.Fields {
		if field.IsArray {
			// It's an array.
			propertyContent := make(map[string]interface{})
			properties[field.Name] = propertyContent

			if field.GoType == "uint8" {
				propertyContent["type"] = "string"
			} else {
				// Arrays all have a type of 'array', regardless of that the hold, then the 'item' keyword determines what type goes in the array.
				propertyContent["type"] = "array"
				arrayItems := make(map[string]interface{})
				propertyContent["items"] = arrayItems

				// Need to handle each type appropriately.
				if field.IsBuiltin {
					if field.Type == "string" {
						arrayItems["type"] = "string"
					} else if field.Type == "time" {
						timeItems := make(map[string]interface{})
						timeItems["sec"] = map[string]string{"type": "integer"}
						timeItems["nsec"] = map[string]string{"type": "integer"}
						arrayItems["type"] = "object"
						arrayItems["properties"] = timeItems
					} else if field.Type == "duration" {
						timeItems := make(map[string]interface{})
						timeItems["sec"] = map[string]string{"type": "integer"}
						timeItems["nsec"] = map[string]string{"type": "integer"}
						arrayItems["type"] = "object"
						arrayItems["properties"] = timeItems
					} else {
						// It's a primitive.
						var jsonType string
						if field.GoType == "int8" || field.GoType == "int16" || field.GoType == "uint16" ||
							field.GoType == "int32" || field.GoType == "uint32" || field.GoType == "int64" || field.GoType == "uint64" {
							jsonType = "integer"
						} else if field.GoType == "float32" || field.GoType == "float64" {
							jsonType = "number"
						} else if field.GoType == "bool" {
							jsonType = "bool"
						} else {
							// Something went wrong.
							return nil, errors.New("we haven't implemented this primitive yet")
						}
						arrayItems["type"] = jsonType
					}

				} else {
					// It's another nested message.

					// Generate the nested type.
					msgType, err := newDynamicMessageTypeNested(field.Type, field.Package, nil, nil)
					if err != nil {
						return nil, errors.Wrap(err, "Schema Field: "+field.Name)
					}

					// Recursively generate schema information for the nested type.
					schemaElement, err := msgType.generateJSONSchemaProperties(topic + Sep + field.Name)
					if err != nil {
						return nil, errors.Wrap(err, "Schema Field:"+field.Name)
					}
					arrayItems["type"] = schemaElement
				}
			}
		} else {
			// It's a scalar.
			if field.IsBuiltin {
				propertyContent := make(map[string]interface{})
				properties[field.Name] = propertyContent

				if field.Type == "string" {
					propertyContent["type"] = "string"
				} else if field.Type == "time" {
					timeItems := make(map[string]interface{})
					timeItems["sec"] = map[string]string{"type": "integer"}
					timeItems["nsec"] = map[string]string{"type": "integer"}
					propertyContent["type"] = "object"
					propertyContent["properties"] = timeItems
				} else if field.Type == "duration" {
					timeItems := make(map[string]interface{})
					timeItems["sec"] = map[string]string{"type": "integer"}
					timeItems["nsec"] = map[string]string{"type": "integer"}
					propertyContent["type"] = "object"
					propertyContent["properties"] = timeItems
				} else {
					// It's a primitive.
					var jsonType string
					if field.GoType == "int8" || field.GoType == "uint8" || field.GoType == "int16" || field.GoType == "uint16" ||
						field.GoType == "int32" || field.GoType == "uint32" || field.GoType == "int64" || field.GoType == "uint64" {
						jsonType = "integer"
						jsonType = "integer"
						jsonType = "integer"
					} else if field.GoType == "float32" || field.GoType == "float64" {
						jsonType = "number"
					} else if field.GoType == "bool" {
						jsonType = "bool"
					} else {
						// Something went wrong.
						return nil, errors.New("we haven't implemented this primitive yet")
					}
					propertyContent["type"] = jsonType
				}
			} else {
				// It's another nested message.

				// Generate the nested type.
				msgType, err := newDynamicMessageTypeNested(field.Type, field.Package, nil, nil)
				if err != nil {
					return nil, errors.Wrap(err, "Schema Field: "+field.Name)
				}

				// Recursively generate schema information for the nested type.
				schemaElement, err := msgType.generateJSONSchemaProperties(topic + Sep + field.Name)
				if err != nil {
					return nil, errors.Wrap(err, "Schema Field:"+field.Name)
				}
				properties[field.Name] = schemaElement
			}
		}
	}

	// All done.
	return schemaItems, nil
}

//	DynamicMessage

// MarshalJSON provides a custom implementation of JSON marshalling, only the message payload is represented in compact form. Verification provided in dynamic_message_json_test.go.
// The marshalled JSON must match the schema generated by GenerateJSONSchema().
func (m *DynamicMessage) MarshalJSON() ([]byte, error) {
	// Confirm the pointers are valid.
	if err := messagePointerError(m); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, m.dynamicType.jsonPrealloc)

	buf = append(buf, byte('{'))
	for i, field := range m.dynamicType.spec.Fields {
		// Marshal the JSON name key.
		if i > 0 {
			buf = append(buf, byte(','))
		}
		buf = strconv.AppendQuote(buf, field.Name)
		buf = append(buf, byte(':'))

		// Marshal the value.
		v, ok := m.data[field.Name]
		if !ok {
			return nil, errors.Wrap(errors.New("key not in data"), "key: "+field.Name)
		}
		var err error
		if field.IsArray {
			err = marshalArrayValue(&field, v, &buf)
		} else {
			err = marshalSingularValue(&field, v, &buf)
		}
		if err != nil {
			return nil, errors.Wrap(err, "field: "+field.Name)
		}
	}

	buf = append(buf, byte('}'))

	if length := len(buf); length > m.dynamicType.jsonPrealloc {
		m.dynamicType.jsonPrealloc = length
	}

	return buf, nil
}

// UnmarshalJSON provides a custom implementation of JSON unmarshalling. Verification provided in dynamic_message_json_test.go.
func (m *DynamicMessage) UnmarshalJSON(buf []byte) (err error) {
	// Confirm the pointers are valid.
	if err := messagePointerError(m); err != nil {
		return err
	}

	// JSON unmarshalling. Iterates and executes the callback for each item found in buf.
	return jsonparser.ObjectEach(buf, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		fieldExists := false
		var field libgengo.Field

		// Find message spec field that matches JSON key
		keyString := string(key)
		for _, specField := range m.dynamicType.spec.Fields {
			if keyString == specField.Name {
				field = specField
				fieldExists = true
			}
		}
		if fieldExists == false {
			return errors.New("Field Unknown: " + string(key))
		}
		switch dataType {

		case jsonparser.String:
			if field.IsArray && field.BuiltInType != libgengo.Uint8 {
				return errors.Wrap(errors.New("attempted to unmarshal array as singular"), "field: "+field.Name+" value: "+string(value))
			}
			var result interface{}
			if err := unmarshalString(value, &field, &result); err != nil {
				return errors.Wrap(err, "field: "+field.Name)
			}
			m.data[field.Name] = result

		case jsonparser.Number: // We have a JSON number; expect a float or integer.
			if field.IsArray {
				return errors.Wrap(errors.New("attempted to unmarshal array as singular"), "field: "+field.Name+" value: "+string(value))
			}
			var result interface{}
			if err := unmarshalNumber(value, &field, &result); err != nil {
				return errors.Wrap(err, "field: "+field.Name)
			}
			m.data[field.Name] = result

		case jsonparser.Boolean:
			if field.IsArray {
				return errors.Wrap(errors.New("attempted to unmarshal array as singular"), "field: "+field.Name+" value: "+string(value))
			}
			if field.BuiltInType != libgengo.Bool {
				return errors.Wrap(errors.New("attempted to parse "+field.Type+" as bool"), "field: "+field.Name+" value: "+string(value))
			}
			value, err := jsonparser.ParseBoolean(value)
			if err != nil {
				return errors.Wrap(err, "field: "+field.Name)
			}
			m.data[field.Name] = value

		case jsonparser.Object:
			if field.IsArray {
				return errors.Wrap(errors.New("attempted to unmarshal array as singular"), "field: "+field.Name+" value: "+string(value))
			}
			var result interface{}
			if err := unmarshalObject(m.dynamicType, value, &field, &result); err != nil {
				return errors.Wrap(err, "field: "+field.Name)
			}
			m.data[field.Name] = result

		case jsonparser.Array:
			if field.IsArray == false {
				return errors.Wrap(errors.New("attempted to unmarshal singular as array"), "field: "+field.Name+" value: "+string(value))
			}
			var result interface{}
			if err := unmarshalArray(m.dynamicType, value, &field, &result); err != nil {
				return errors.Wrap(err, "field: "+field.Name)
			}
			m.data[field.Name] = result
		default:
			// We do nothing here as blank fields may return value type NotExist or Null
			return errors.Wrap(err, "Null field: "+string(key))
		}

		return err
	})
}

// DEFINE PRIVATE STATIC FUNCTIONS.

// Returns error if dynamic message has invalid pointer data.
func messagePointerError(m *DynamicMessage) error {
	if m == nil {
		return errors.New("nil pointer to DynamicMessage")
	}
	if m.dynamicType == nil {
		return errors.New("nil pointer to dynamicType")
	}
	if m.data == nil {
		return errors.New("nil pointer to dynamicType")
	}
	if m.dynamicType.nested == nil {
		return errors.New("nil pointer to dynamicType nested map")
	}
	if m.dynamicType.spec == nil {
		return errors.New("nil pointer to MsgSpec")
	}
	if m.dynamicType.spec.Fields == nil {
		return errors.New("nil pointer to Fields")
	}
	return nil
}

// Marshalling Helpers.

func marshalFloat(f float64, buf *[]byte, bits int) {
	if math.IsNaN(f) {
		*buf = strconv.AppendQuote(*buf, "nan")
	} else if math.IsInf(f, 1) {
		*buf = strconv.AppendQuote(*buf, "+inf")
	} else if math.IsInf(f, -1) {
		*buf = strconv.AppendQuote(*buf, "-inf")
	} else {
		*buf = strconv.AppendFloat(*buf, f, byte('g'), -1, bits)
	}
}

func marshalSecNSec(sec uint64, nsec uint64, buf *[]byte) {
	*buf = append(*buf, []byte("{\"sec\":")...)
	*buf = strconv.AppendUint(*buf, sec, 10)
	*buf = append(*buf, []byte(",\"nsec\":")...)
	*buf = strconv.AppendUint(*buf, nsec, 10)
	*buf = append(*buf, byte('}'))
}

func marshalArrayUint8(v interface{}, buf *[]byte) error {
	slice, ok := v.([]uint8)
	if ok == false {
		return newTypeError(v, "[]uint8")
	}
	*buf = append(*buf, byte('"'))
	encodedLen := base64.StdEncoding.EncodedLen(len(slice))
	if (cap(*buf) - len(*buf)) > encodedLen {
		dst := (*buf)[len(*buf) : len(*buf)+encodedLen]
		base64.StdEncoding.Encode(dst, slice)
		*buf = (*buf)[:len(*buf)+encodedLen]
	} else {
		dst := make([]byte, encodedLen)
		base64.StdEncoding.Encode(dst, slice)
		*buf = append(*buf, dst...)
	}
	*buf = append(*buf, byte('"'))
	return nil
}

func newTypeError(v interface{}, expected string) error {
	return errors.New("has type " + reflect.TypeOf(v).Name() + ", expected " + expected)
}

func marshalArrayValue(field *libgengo.Field, v interface{}, buf *[]byte) error {
	if field.BuiltInType == libgengo.Uint8 { // Special case, is marshalled as base64.
		return marshalArrayUint8(v, buf)
	}

	*buf = append(*buf, byte('['))
	if field.IsBuiltin == false {
		// The type encapsulates an array of ROS messages, so we marshal the DynamicMessages.
		if nestedArray, ok := v.([]Message); ok {
			for i, nested := range nestedArray {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				nestedDynamicMessage, ok := nested.(*DynamicMessage)
				if ok == false {
					return newTypeError(nested, "DynamicMessage")
				}

				nestedbuf, err := nestedDynamicMessage.MarshalJSON()
				if err != nil {
					return err
				}
				*buf = append(*buf, nestedbuf...)
			}
		} else {
			return newTypeError(v, "[]Message")
		}
	} else {
		switch field.BuiltInType {
		case libgengo.Bool:
			slice, ok := v.([]bool)
			if ok == false {
				return newTypeError(v, "[]bool")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				if item == true {
					*buf = append(*buf, []byte("true")...)
				} else {
					*buf = append(*buf, []byte("false")...)
				}
			}
		case libgengo.Int8:
			slice, ok := v.([]int8)
			if ok == false {
				return newTypeError(v, "[]int8")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendInt(*buf, int64(item), 10)
			}
		case libgengo.Int16:
			slice, ok := v.([]int16)
			if ok == false {
				return newTypeError(v, "[]int16")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendInt(*buf, int64(item), 10)
			}
		case libgengo.Int32:
			slice, ok := v.([]int32)
			if ok == false {
				return newTypeError(v, "[]int32")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendInt(*buf, int64(item), 10)
			}
		case libgengo.Int64:
			slice, ok := v.([]int64)
			if ok == false {
				return newTypeError(v, "[]int64")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendInt(*buf, item, 10)
			}
		case libgengo.Uint16:
			slice, ok := v.([]uint16)
			if ok == false {
				return newTypeError(v, "[]uint16")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendUint(*buf, uint64(item), 10)
			}
		case libgengo.Uint32:
			slice, ok := v.([]uint32)
			if ok == false {
				return newTypeError(v, "[]uint32")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendUint(*buf, uint64(item), 10)
			}
		case libgengo.Uint64:
			slice, ok := v.([]uint64)
			if ok == false {
				return newTypeError(v, "[]uint64")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendUint(*buf, item, 10)
			}
		case libgengo.Float32:
			slice, ok := v.([]JsonFloat32)
			if ok == false {
				return newTypeError(v, "[]JsonFloat32")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				marshalFloat(float64(item.F), &*buf, 32)
			}
		case libgengo.Float64:
			slice, ok := v.([]JsonFloat64)
			if ok == false {
				return newTypeError(v, "[]JsonFloat64")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				marshalFloat(item.F, &*buf, 64)
			}
		case libgengo.String:
			slice, ok := v.([]string)
			if ok == false {
				return newTypeError(v, "[]string")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				*buf = strconv.AppendQuote(*buf, item)
			}
		case libgengo.Time:
			slice, ok := v.([]Time)
			if ok == false {
				return newTypeError(v, "[]Time")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				marshalSecNSec(uint64(item.Sec), uint64(item.NSec), &*buf)
			}
		case libgengo.Duration:
			slice, ok := v.([]Duration)
			if ok == false {
				return newTypeError(v, "[]Duration")
			}
			for i, item := range slice {
				if i > 0 {
					*buf = append(*buf, byte(','))
				}
				marshalSecNSec(uint64(item.Sec), uint64(item.NSec), &*buf)
			}
		default:
			// Something went wrong.
			return errors.New("unknown builtin type" + field.GoType)
		}
	}
	// Success.
	*buf = append(*buf, byte(']'))
	return nil
}

func marshalSingularValue(field *libgengo.Field, v interface{}, buf *[]byte) error {
	if field.IsBuiltin == false {
		// The type encapsulates another ROS message, so we marshal the DynamicMessage.
		if nested, ok := v.(*DynamicMessage); ok {
			nestedBuf, err := nested.MarshalJSON()
			if err != nil {
				return err
			}
			*buf = append(*buf, nestedBuf...)
		} else {
			return newTypeError(v, "DynamicMessage")
		}
		return nil
	}
	switch field.BuiltInType {
	case libgengo.Bool:
		value, ok := v.(bool)
		if ok == false {
			return newTypeError(v, "int8")
		}
		if value {
			*buf = append(*buf, []byte("true")...)
		} else {
			*buf = append(*buf, []byte("false")...)
		}
	case libgengo.Int8:
		value, ok := v.(int8)
		if ok == false {
			return newTypeError(v, "int8")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Int16:
		value, ok := v.(int16)
		if ok == false {
			return newTypeError(v, "int16")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Int32:
		value, ok := v.(int32)
		if ok == false {
			return newTypeError(v, "int32")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Int64:
		value, ok := v.(int64)
		if ok == false {
			return newTypeError(v, "int32")
		}
		*buf = strconv.AppendInt(*buf, value, 10)
	case libgengo.Uint8:
		value, ok := v.(uint8)
		if ok == false {
			return newTypeError(v, "uint8")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Uint16:
		value, ok := v.(uint16)
		if ok == false {
			return newTypeError(v, "uint16")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Uint32:
		value, ok := v.(uint32)
		if ok == false {
			return newTypeError(v, "uint32")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Uint64:
		value, ok := v.(uint64)
		if ok == false {
			return newTypeError(v, "uint64")
		}
		*buf = strconv.AppendInt(*buf, int64(value), 10)
	case libgengo.Float32:
		value, ok := v.(JsonFloat32)
		if ok == false {
			return newTypeError(v, "JsonFloat32")
		}
		marshalFloat(float64(value.F), &*buf, 32)
	case libgengo.Float64:
		value, ok := v.(JsonFloat64)
		if ok == false {
			return newTypeError(v, "JsonFloat64")
		}
		marshalFloat(value.F, &*buf, 64)
	case libgengo.String:
		value, ok := v.(string)
		if ok == false {
			return newTypeError(v, "string")
		}
		*buf = strconv.AppendQuote(*buf, value)
	case libgengo.Time:
		value, ok := v.(Time)
		if ok == false {
			return newTypeError(v, "Time")
		}
		marshalSecNSec(uint64(value.Sec), uint64(value.NSec), &*buf)
	case libgengo.Duration:
		value, ok := v.(Duration)
		if ok == false {
			return newTypeError(v, "Duration")
		}
		marshalSecNSec(uint64(value.Sec), uint64(value.NSec), &*buf)
	default:
		// Something went wrong.
		return errors.New("unknown builtin type " + field.GoType)
	}
	return nil
}

// Unmarshalling Helpers.

func unmarshalSecNSecObject(marshalled []byte) (sec uint32, nsec uint32, err error) {
	var tempSec int64
	var tempNSec int64
	hasSec := false
	hasNSec := false

	// Expects the object to be {"sec":n,"nsec":n} (although, order doesn't matter).
	err = jsonparser.ObjectEach(marshalled, func(k []byte, v []byte, dataType jsonparser.ValueType, offset int) error {
		fmt.Printf("unmarshalSecNSecObject:  k: %+v, v: %+v, dataType: %+v, offset: %v\n", k, v, dataType, offset)
		var err error
		switch string(k) {
		case "sec", "secs", "Sec", "Secs":
			tempSec, err = jsonparser.ParseInt(v)
			if err == nil {
				hasSec = true
			}
		case "nsec", "nsecs", "NSec", "NSecs":
			tempNSec, err = jsonparser.ParseInt(v)
			if err == nil {
				hasNSec = true
			}
		default:
			err = errors.New("unknown key " + string(k))
		}
		return err
	})
	if err == nil {
		if hasSec == false {
			return 0, 0, errors.Wrap(errors.New("object had no Sec field"), "obj: "+string(marshalled))
		}
		if hasNSec == false {
			return 0, 0, errors.Wrap(errors.New("object had no NSec field"), "obj: "+string(marshalled))
		}
	}
	fmt.Printf("returning: sec: %v, nsec: %v\n", sec, nsec)
	return uint32(tempSec), uint32(tempNSec), err
}

func unmarshalString(value []byte, field *libgengo.Field, dest *interface{}) error {
	switch field.BuiltInType {
	case libgengo.Uint8: // We have a byte array encoded as JSON string.
		data, err := base64.StdEncoding.DecodeString(string(value))
		if err != nil {
			return err
		}
		*dest = data
	case libgengo.Float32: // We have marshalled a float32 as a string.
		floatValue, err := strconv.ParseFloat(string(value), 32)
		if err != nil {
			return err
		}
		*dest = JsonFloat32{F: float32(floatValue)}
	case libgengo.Float64: // We have marshalled a float64 as a string.
		floatValue, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return err
		}
		*dest = JsonFloat64{F: floatValue}
	case libgengo.String:
		stringValue, err := strconv.Unquote(`"` + string(value) + `"`)
		if err != nil {
			return err
		}
		*dest = string(stringValue)
	default:
		return errors.New("unexpected json string")
	}
	return nil
}

func unmarshalNumber(value []byte, field *libgengo.Field, dest *interface{}) error {
	var intValue int64
	var floatValue float64
	var err error
	//We have a float to parse
	if field.BuiltInType == libgengo.Float64 || field.BuiltInType == libgengo.Float32 {
		floatValue, err = strconv.ParseFloat(string(value), 64)
		if err != nil {
			return err
		}
	} else { //We have an int to parse
		intValue, err = jsonparser.ParseInt(value)
		if err != nil {
			return err
		}
	}
	//Copy number value to message field
	switch field.BuiltInType {
	case libgengo.Int8:
		*dest = int8(intValue)
	case libgengo.Int16:
		*dest = int16(intValue)
	case libgengo.Int32:
		*dest = int32(intValue)
	case libgengo.Int64:
		*dest = int64(intValue)
	case libgengo.Uint8:
		*dest = uint8(intValue)
	case libgengo.Uint16:
		*dest = uint16(intValue)
	case libgengo.Uint32:
		*dest = uint32(intValue)
	case libgengo.Uint64:
		*dest = uint64(intValue)
	case libgengo.Float32:
		*dest = JsonFloat32{F: float32(floatValue)}
	case libgengo.Float64:
		*dest = JsonFloat64{F: floatValue}
	default:
		return errors.New("unexpected number")
	}
	return nil
}

func unmarshalObject(msgType *DynamicMessageType, value []byte, field *libgengo.Field, dest *interface{}) error {
	if field.IsBuiltin {
		switch field.BuiltInType {
		case libgengo.Time:
			sec, nsec, err := unmarshalSecNSecObject(value)
			if err != nil {
				return err
			}
			*dest = NewTime(sec, nsec)
		case libgengo.Duration:
			sec, nsec, err := unmarshalSecNSecObject(value)
			if err != nil {
				return err
			}
			*dest = NewDuration(sec, nsec)
		default:
			return errors.New("unexpected object")
		}
	} else {
		// We have a nested message.
		msgType, err := msgType.getNestedTypeFromField(field)
		if err != nil {
			return err
		}
		msg := msgType.NewDynamicMessage()
		if err = msg.UnmarshalJSON(value); err != nil {
			return err
		}
		*dest = msg
	}
	return nil
}

func unmarshalArray(msgType *DynamicMessageType, value []byte, field *libgengo.Field, dest *interface{}) error {
	var err error
	var unmarshalledLength int

	size := field.ArrayLen
	if size < 0 {
		size = 0
	}
	if field.IsBuiltin {
		switch field.BuiltInType {
		case libgengo.Bool:
			array := make([]bool, 0, size)
			err = unmarshalBoolArray(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Int8:
			array := make([]int8, 0, size)
			err = unmarshalInt8Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Int16:
			array := make([]int16, 0, size)
			err = unmarshalInt16Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Int32:
			array := make([]int32, 0, size)
			err = unmarshalInt32Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Int64:
			array := make([]int64, 0, size)
			err = unmarshalInt64Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Uint8:
			// We expect this to be base64 encoded. Handle it anyway.
			array := make([]uint8, 0, size)
			err = unmarshalUint8Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Uint16:
			array := make([]uint16, 0, size)
			err = unmarshalUint16Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Uint32:
			array := make([]uint32, 0, size)
			err = unmarshalUint32Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Uint64:
			array := make([]uint64, 0, size)
			err = unmarshalUint64Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Float32:
			array := make([]JsonFloat32, 0, size)
			err = unmarshalFloat32Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Float64:
			array := make([]JsonFloat64, 0, size)
			err = unmarshalFloat64Array(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.String:
			array := make([]string, 0, size)
			err = unmarshalStringArray(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Time:
			array := make([]Time, 0, size)
			err = unmarshalTimeArray(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		case libgengo.Duration:
			array := make([]Duration, 0, size)
			err = unmarshalDurationArray(value, &array)
			unmarshalledLength = len(array)
			*dest = array
		default:
			return errors.New("unexpected object is builtin")
		}
	} else { // goType is a nested Message array
		msgType, err := msgType.getNestedTypeFromField(field)
		if err != nil {
			return err
		}
		array := make([]Message, 0, size)
		err = unmarshalMessageArray(value, &array, msgType)
		unmarshalledLength = len(array)
		*dest = array
	}
	if field.ArrayLen > 0 && unmarshalledLength != field.ArrayLen {
		return errors.Wrap(errors.New("fixed array size does not match unmarshalled size"), "json: "+string(value)+", fixed size: "+strconv.Itoa(field.ArrayLen))
	}
	return err
}

func unmarshalBoolArray(value []byte, array *[]bool) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Boolean {
			var data bool
			data, err = jsonparser.ParseBoolean(value)
			if err != nil {
				return
			}
			*array = append(*array, data)
		} else {
			err = errors.New("unexpected type, expecting bool")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalInt8Array(value []byte, array *[]int8) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, int8(data))
		} else {
			err = errors.New("unexpected type, expecting int8")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalInt16Array(value []byte, array *[]int16) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, int16(data))
		} else {
			err = errors.New("unexpected type, expecting int16")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalInt32Array(value []byte, array *[]int32) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, int32(data))
		} else {
			err = errors.New("unexpected type, expecting int32")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalInt64Array(value []byte, array *[]int64) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, int64(data))
		} else {
			err = errors.New("unexpected type, expecting int64")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalUint8Array(value []byte, array *[]uint8) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, uint8(data))
		} else {
			err = errors.New("unexpected type, expecting uint8")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalUint16Array(value []byte, array *[]uint16) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, uint16(data))
		} else {
			err = errors.New("unexpected type, expecting uint16")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalUint32Array(value []byte, array *[]uint32) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, uint32(data))
		} else {
			err = errors.New("unexpected type, expecting uint32")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalUint64Array(value []byte, array *[]uint64) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Number {
			var data int64
			data, err = jsonparser.ParseInt(value)
			if err != nil {
				return
			}
			*array = append(*array, uint64(data))
		} else {
			err = errors.New("unexpected type, expecting uint64")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalFloat32Array(value []byte, array *[]JsonFloat32) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.String || dataType == jsonparser.Number {
			var floatValue float64
			floatValue, err = strconv.ParseFloat(string(value), 32)
			if err != nil {
				return
			}
			*array = append(*array, JsonFloat32{F: float32(floatValue)})
		} else {
			err = errors.New("unexpected type, expecting float64")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalFloat64Array(value []byte, array *[]JsonFloat64) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.String || dataType == jsonparser.Number {
			var floatValue float64
			floatValue, err = strconv.ParseFloat(string(value), 64)
			if err != nil {
				return
			}
			*array = append(*array, JsonFloat64{F: floatValue})
		} else {
			err = errors.New("unexpected type, expecting float64")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalStringArray(value []byte, array *[]string) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.String {
			var stringValue string
			stringValue, err = strconv.Unquote(`"` + string(value) + `"`)
			if err != nil {
				return
			}
			*array = append(*array, string(stringValue))
		} else {
			err = errors.New("unexpected type, expecting string")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalTimeArray(value []byte, array *[]Time) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Object {
			sec, nsec, err := unmarshalSecNSecObject(value)
			if err != nil {
				return
			}
			*array = append(*array, NewTime(sec, nsec))
		} else {
			err = errors.New("unexpected type, expecting time object")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalDurationArray(value []byte, array *[]Duration) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Object {
			sec, nsec, err := unmarshalSecNSecObject(value)
			if err != nil {
				return
			}
			*array = append(*array, NewDuration(sec, nsec))
		} else {
			err = errors.New("unexpected type, expecting duration object")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

func unmarshalMessageArray(value []byte, array *[]Message, msgType *DynamicMessageType) error {
	var err error
	arrayHandler := func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		if err != nil {
			return // Stop processing if there is an error.
		}
		if dataType == jsonparser.Object {
			msg := msgType.NewDynamicMessage()
			if err = msg.UnmarshalJSON(value); err != nil {
				return
			}
			*array = append(*array, msg)
		} else {
			err = errors.New("unexpected type, expecting message object")
		}
	}
	jsonparser.ArrayEach(value, arrayHandler)
	return err
}

// DEFINE PRIVATE RECEIVER FUNCTIONS.

// ALL DONE.
