package ros

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	gengo "github.com/team-rocos/rosgo/libgengo"
)

func TestDynamicMessage_JSON_primitives(t *testing.T) {
	testCases := []struct {
		fields     []gengo.Field
		data       map[string]interface{}
		marshalled string
	}{
		// Singular Values.
		// - Unsigned integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)},
			data:       map[string]interface{}{"u8": uint8(0x12)},
			marshalled: `{"u8":18}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", false, 0)},
			data:       map[string]interface{}{"u16": uint16(0x8001)},
			marshalled: `{"u16":32769}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", false, 0)},
			data:       map[string]interface{}{"u32": uint32(0x80000001)},
			marshalled: `{"u32":2147483649}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", false, 0)},
			data:       map[string]interface{}{"u64": uint64(0x8000000000001)}, // Note: Can only represent up to 52-bits due to JSON number representation!
			marshalled: `{"u64":2251799813685249}`,
		},
		// - Signed integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int8", "i8", false, 0)},
			data:       map[string]interface{}{"i8": int8(-20)},
			marshalled: `{"i8":-20}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", false, 0)},
			data:       map[string]interface{}{"i16": int16(-20_000)},
			marshalled: `{"i16":-20000}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", false, 0)},
			data:       map[string]interface{}{"i32": int32(-2_000_000_000)},
			marshalled: `{"i32":-2000000000}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", false, 0)},
			data:       map[string]interface{}{"i64": int64(-2_000_000_000_000_000_000)},
			marshalled: `{"i64":-2000000000000000000}`,
		},
		// - Booleans.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			data:       map[string]interface{}{"b": false},
			marshalled: `{"b":false}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			data:       map[string]interface{}{"b": true},
			marshalled: `{"b":true}`,
		},
		// - Floats.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{0.0}},
			marshalled: `{"f32":0}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{-1.125}},
			marshalled: `{"f32":-1.125}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{-2.3e8}},
			marshalled: `{"f32":-2.3e+08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{2.3e-8}},
			marshalled: `{"f32":2.3e-08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{float32(math.Inf(1))}},
			marshalled: `{"f32":"+inf"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:       map[string]interface{}{"f32": JsonFloat32{float32(math.Inf(-1))}},
			marshalled: `{"f32":"-inf"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:       map[string]interface{}{"f64": JsonFloat64{-1.125}},
			marshalled: `{"f64":-1.125}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:       map[string]interface{}{"f64": JsonFloat64{-2.3e8}},
			marshalled: `{"f64":-2.3e+08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:       map[string]interface{}{"f64": JsonFloat64{2.3e-8}},
			marshalled: `{"f64":2.3e-08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:       map[string]interface{}{"f64": JsonFloat64{math.Inf(1)}},
			marshalled: `{"f64":"+inf"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:       map[string]interface{}{"f64": JsonFloat64{math.Inf(-1)}},
			marshalled: `{"f64":"-inf"}`,
		},
		// - Strings.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			data:       map[string]interface{}{"s": ""},
			marshalled: `{"s":""}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			data:       map[string]interface{}{"s": "N0t  empty "},
			marshalled: `{"s":"N0t  empty "}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			data:       map[string]interface{}{"s": "\tha\nos g\\ove"},
			marshalled: `{"s":"\tha\nos g\\ove"}`,
		},
		// - Time and Duration.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			data:       map[string]interface{}{"t": NewTime(0xfeedf00d, 0x1337beef)},
			marshalled: `{"t":{"sec":4277006349,"nsec":322420463}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, 0)},
			data:       map[string]interface{}{"d": NewDuration(0x40302010, 0x00706050)},
			marshalled: `{"d":{"sec":1076895760,"nsec":7364688}}`,
		},
		// Fixed and Dynamic arrays.
		// - Unsigned integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", true, 8)},
			data:       map[string]interface{}{"u8": []uint8{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12}},
			marshalled: `{"u8":"8N68mnhWNBI="}`, // From https://base64.guru/converter/encode/hex
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, 5)},
			data:       map[string]interface{}{"u16": []uint16{0xf0de, 0xbc9a, 0x7856, 0x3412, 0x0}},
			marshalled: `{"u16":[61662,48282,30806,13330,0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, 3)},
			data:       map[string]interface{}{"u32": []uint32{0xf0debc9a, 0x78563412, 0x0}},
			marshalled: `{"u32":[4041129114,2018915346,0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, 3)},
			data:       map[string]interface{}{"u64": []uint64{0x8000000000001, 0x78563412, 0x0}},
			marshalled: `{"u64":[2251799813685249,2018915346,0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"u8": []uint8{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12}},
			marshalled: `{"u8":"8N68mnhWNBI="}`, // From https://base64.guru/converter/encode/hex
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"u16": []uint16{0xf0de, 0xbc9a, 0x7856, 0x3412, 0x0}},
			marshalled: `{"u16":[61662,48282,30806,13330,0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"u32": []uint32{0xf0debc9a, 0x78563412, 0x0}},
			marshalled: `{"u32":[4041129114,2018915346,0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"u64": []uint64{0x8000000000001, 0x78563412, 0x0}},
			marshalled: `{"u64":[2251799813685249,2018915346,0]}`,
		},
		// - Signed integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int8", "i8", true, 8)},
			data:       map[string]interface{}{"i8": []int8{-128, -55, -1, 0, 1, 7, 77, 127}},
			marshalled: `{"i8":[-128,-55,-1,0,1,7,77,127]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, 7)},
			data:       map[string]interface{}{"i16": []int16{-32768, -129, -1, 0, 1, 128, 32767}},
			marshalled: `{"i16":[-32768,-129,-1,0,1,128,32767]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, 9)},
			data:       map[string]interface{}{"i32": []int32{-2147483648, -32768, -129, -1, 0, 1, 128, 32767, 2147483647}},
			marshalled: `{"i32":[-2147483648,-32768,-129,-1,0,1,128,32767,2147483647]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, 11)},
			data:       map[string]interface{}{"i64": []int64{-2251799813685249, -2147483648, -32768, -129, -1, 0, 1, 128, 32767, 2147483647, 2251799813685249}},
			marshalled: `{"i64":[-2251799813685249,-2147483648,-32768,-129,-1,0,1,128,32767,2147483647,2251799813685249]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int8", "i8", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"i8": []int8{-128, -55, -1, 0, 1, 7, 77, 127}},
			marshalled: `{"i8":[-128,-55,-1,0,1,7,77,127]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"i16": []int16{-32768, -129, -1, 0, 1, 128, 32767}},
			marshalled: `{"i16":[-32768,-129,-1,0,1,128,32767]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"i32": []int32{-2147483648, -32768, -129, -1, 0, 1, 128, 32767, 2147483647}},
			marshalled: `{"i32":[-2147483648,-32768,-129,-1,0,1,128,32767,2147483647]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"i64": []int64{-2251799813685249, -2147483648, -32768, -129, -1, 0, 1, 128, 32767, 2147483647, 2251799813685249}},
			marshalled: `{"i64":[-2251799813685249,-2147483648,-32768,-129,-1,0,1,128,32767,2147483647,2251799813685249]}`,
		},
		// - Booleans.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 2)},
			data:       map[string]interface{}{"b": []bool{true, false}},
			marshalled: `{"b":[true,false]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"b": []bool{true, false}},
			marshalled: `{"b":[true,false]}`,
		},
		// - Floats.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", true, 6)},
			data:       map[string]interface{}{"f32": []JsonFloat32{{-1.125}, {3.3e3}, {7.7e7}, {9.9e-9}, {float32(math.Inf(1))}, {float32(math.Inf(-1))}}},
			marshalled: `{"f32":[-1.125,3300,7.7e+07,9.9e-09,"+inf","-inf"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", true, 6)},
			data:       map[string]interface{}{"f64": []JsonFloat64{{-1.125}, {3.3e3}, {7.7e7}, {9.9e-9}, {math.Inf(1)}, {math.Inf(-1)}}},
			marshalled: `{"f64":[-1.125,3300,7.7e+07,9.9e-09,"+inf","-inf"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"f32": []JsonFloat32{{-1.125}, {3.3e3}, {7.7e7}, {9.9e-9}, {float32(math.Inf(1))}, {float32(math.Inf(-1))}}},
			marshalled: `{"f32":[-1.125,3300,7.7e+07,9.9e-09,"+inf","-inf"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"f64": []JsonFloat64{{-1.125}, {3.3e3}, {7.7e7}, {9.9e-9}, {math.Inf(1)}, {math.Inf(-1)}}},
			marshalled: `{"f64":[-1.125,3300,7.7e+07,9.9e-09,"+inf","-inf"]}`,
		},
		// - Strings.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", true, 5)},
			data:       map[string]interface{}{"s": []string{"", "n0t empty  ", "new\nline", "\ttabbed", "s\\ash"}},
			marshalled: `{"s":["","n0t empty  ","new\nline","\ttabbed","s\\ash"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"s": []string{"", "n0t empty  ", "new\nline", "\ttabbed", "s\\ash"}},
			marshalled: `{"s":["","n0t empty  ","new\nline","\ttabbed","s\\ash"]}`,
		},
		// - Time.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", true, 2)},
			data:       map[string]interface{}{"t": []Time{NewTime(0xfeedf00d, 0x1337beef), NewTime(0x1337beef, 0x00706050)}},
			marshalled: `{"t":[{"sec":4277006349,"nsec":322420463},{"sec":322420463,"nsec":7364688}]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"t": []Time{NewTime(0xfeedf00d, 0x1337beef), NewTime(0x1337beef, 0x00706050)}},
			marshalled: `{"t":[{"sec":4277006349,"nsec":322420463},{"sec":322420463,"nsec":7364688}]}`,
		},
		// - Duration.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 2)},
			data:       map[string]interface{}{"d": []Duration{NewDuration(0xfeedf00d, 0x1337beef), NewDuration(0x1337beef, 0x00706050)}},
			marshalled: `{"d":[{"sec":4277006349,"nsec":322420463},{"sec":322420463,"nsec":7364688}]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, -1)}, // Dynamic.
			data:       map[string]interface{}{"d": []Duration{NewDuration(0xfeedf00d, 0x1337beef), NewDuration(0x1337beef, 0x00706050)}},
			marshalled: `{"d":[{"sec":4277006349,"nsec":322420463},{"sec":322420463,"nsec":7364688}]}`,
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\n expected: %v\nerr: %v", testCase.marshalled, err)
		}
		if string(marshalled) != testCase.marshalled {
			t.Fatalf("marshalled data does not equal expected\nmarshalled: %v\nexpected: %v", string(marshalled), testCase.marshalled)
		}

		unmarshalledMessage := testMessageType.NewDynamicMessage()

		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}

		if reflect.DeepEqual(testMessage.data, unmarshalledMessage.data) == false {
			t.Fatalf("original and unmarshalled data mismatch. \n Original: %v \n Unmarshalled: %v \n json: %v", testMessage.data, unmarshalledMessage.data, string(marshalled))
		}
	}
}

// Verify a flat message of ROS built in primitives is marshalled/unmarshalled correctly.
func TestDynamicMessage_JSON_primitiveSet(t *testing.T) {

	fields := []gengo.Field{
		// Singular primitives.
		*gengo.NewField("Testing", "uint8", "singular_u8", false, 0),
		*gengo.NewField("Testing", "uint16", "singular_u16", false, 0),
		*gengo.NewField("Testing", "uint32", "singular_u32", false, 0),
		*gengo.NewField("Testing", "uint64", "singular_u64", false, 0),
		*gengo.NewField("Testing", "int8", "singular_i8", false, 0),
		*gengo.NewField("Testing", "int16", "singular_i16", false, 0),
		*gengo.NewField("Testing", "int32", "singular_i32", false, 0),
		*gengo.NewField("Testing", "int64", "singular_i64", false, 0),
		*gengo.NewField("Testing", "bool", "singular_b", false, 0),
		*gengo.NewField("Testing", "float32", "singular_f32", false, 0),
		*gengo.NewField("Testing", "float64", "singular_f64", false, 0),
		*gengo.NewField("Testing", "string", "singular_s", false, 0),
		*gengo.NewField("Testing", "time", "singular_t", false, 0),
		*gengo.NewField("Testing", "duration", "singular_d", false, 0),
		// Fixed arrays.
		*gengo.NewField("Testing", "uint8", "fixed_u8", true, 8),
		*gengo.NewField("Testing", "uint16", "fixed_u16", true, 4),
		*gengo.NewField("Testing", "uint32", "fixed_u32", true, 2),
		*gengo.NewField("Testing", "uint64", "fixed_u64", true, 1),
		*gengo.NewField("Testing", "int8", "fixed_i8", true, 8),
		*gengo.NewField("Testing", "int16", "fixed_i16", true, 4),
		*gengo.NewField("Testing", "int32", "fixed_i32", true, 2),
		*gengo.NewField("Testing", "int64", "fixed_i64", true, 1),
		*gengo.NewField("Testing", "bool", "fixed_b", true, 8),
		*gengo.NewField("Testing", "float32", "fixed_f32", true, 2),
		*gengo.NewField("Testing", "float64", "fixed_f64", true, 1),
		*gengo.NewField("Testing", "string", "fixed_s", true, 3),
		*gengo.NewField("Testing", "time", "fixed_t", true, 2),
		*gengo.NewField("Testing", "duration", "fixed_d", true, 2),
		// Dynamic arrays.
		*gengo.NewField("Testing", "uint8", "dyn_u8", true, -1),
		*gengo.NewField("Testing", "uint16", "dyn_u16", true, -1),
		*gengo.NewField("Testing", "uint32", "dyn_u32", true, -1),
		*gengo.NewField("Testing", "uint64", "dyn_u64", true, -1),
		*gengo.NewField("Testing", "int8", "dyn_i8", true, -1),
		*gengo.NewField("Testing", "int16", "dyn_i16", true, -1),
		*gengo.NewField("Testing", "int32", "dyn_i32", true, -1),
		*gengo.NewField("Testing", "int64", "dyn_i64", true, -1),
		*gengo.NewField("Testing", "bool", "dyn_b", true, -1),
		*gengo.NewField("Testing", "float32", "dyn_f32", true, -1),
		*gengo.NewField("Testing", "float64", "dyn_f64", true, -1),
		*gengo.NewField("Testing", "string", "dyn_s", true, -1),
		*gengo.NewField("Testing", "time", "dyn_t", true, -1),
		*gengo.NewField("Testing", "duration", "dyn_d", true, -1),
	}

	data := map[string]interface{}{
		"singular_u8":  uint8(0x12),
		"singular_u16": uint16(0x3456),
		"singular_u32": uint32(0x789abcde),
		"singular_u64": uint64(0x123456789abcdef0),
		"singular_i8":  int8(-2),
		"singular_i16": int16(-2),
		"singular_i32": int32(-2),
		"singular_i64": int64(-2),
		"singular_b":   true,
		"singular_f32": JsonFloat32{1234.5678},
		"singular_f64": JsonFloat64{-9876.5432},
		"singular_s":   "Rocos",
		"singular_t":   NewTime(0xfeedf00d, 0x1337beef),
		"singular_d":   NewDuration(0x50607080, 0x10203040),
		"fixed_u8":     []uint8{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		"fixed_u16":    []uint16{0xdef0, 0x9abc, 0x5678, 0x1234},
		"fixed_u32":    []uint32{0x9abcdef0, 0x12345678},
		"fixed_u64":    []uint64{0x123456789abcdef0},
		"fixed_i8":     []int8{-2, -1, 0, 1, 2, 3, 4, 5},
		"fixed_i16":    []int16{-2, -1, 0, 1},
		"fixed_i32":    []int32{-2, 1},
		"fixed_i64":    []int64{-2},
		"fixed_b":      []bool{true, true, false, false, true, false, true, false},
		"fixed_f32":    []JsonFloat32{{1234.5678}, {1234.5678}},
		"fixed_f64":    []JsonFloat64{{-9876.5432}},
		"fixed_s":      []string{"Rocos", "soroc", "croos"},
		"fixed_t":      []Time{NewTime(0xfeedf00d, 0x1337beef), NewTime(0x1337beef, 0x1337f00d)},
		"fixed_d":      []Duration{NewDuration(0x40302010, 0x00706050), NewDuration(0x50607080, 0x10203040)},
		"dyn_u8":       []uint8{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		"dyn_u16":      []uint16{0xdef0, 0x9abc, 0x5678, 0x1234},
		"dyn_u32":      []uint32{0x9abcdef0, 0x12345678},
		"dyn_u64":      []uint64{0x123456789abcdef0},
		"dyn_i8":       []int8{-2, -1, 0, 1, 2, 3, 4, 5},
		"dyn_i16":      []int16{-2, -1, 0, 1},
		"dyn_i32":      []int32{-2, 1},
		"dyn_i64":      []int64{-2},
		"dyn_b":        []bool{true, true, false, false, true, false, true, false},
		"dyn_f32":      []JsonFloat32{{1234.5678}, {1234.5678}},
		"dyn_f64":      []JsonFloat64{{-9876.5432}},
		"dyn_s":        []string{"Rocos", "soroc", "croos"},
		"dyn_t":        []Time{NewTime(0xfeedf00d, 0x1337beef), NewTime(0x1337beef, 0x1337f00d)},
		"dyn_d":        []Duration{NewDuration(0x40302010, 0x00706050), NewDuration(0x50607080, 0x10203040)},
	}

	testMessageType := &DynamicMessageType{
		spec:         generateTestSpec(fields),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := &DynamicMessage{
		dynamicType: testMessageType,
		data:        data,
	}

	marshalled, err := json.Marshal(testMessage)
	if err != nil {
		t.Fatalf("failed to marshal dynamic message\nerr: %v", err)
	}

	unmarshalledMessage := testMessageType.NewDynamicMessage()

	if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
		t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", string(marshalled), err)
	}

	if reflect.DeepEqual(testMessage.data, unmarshalledMessage.data) == false {
		t.Fatalf("original and unmarshalled data mismatch. \n Original: %v \n Unmarshalled: %v \n json: %v", testMessage.data, unmarshalledMessage.data, string(marshalled))
	}
}

// Marshalling dynamic message strings is equivalent to the default marshaller.
func TestDynamicMessage_marshalJSON_strings(t *testing.T) {

	testCases := []struct {
		fields []gengo.Field
		data   map[string]interface{}
	}{
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": "hexidecimal non-printing character \x00 escape"},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", true, 1)},
			data:   map[string]interface{}{"s": []string{"hexidecimal non-printing character \x00 escape"}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": "new\nline"},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": "more \"quotes\""},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": "escape a s\\ash"},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": "t\tab"},
		},
		{
			fields: []gengo.Field{*gengo.NewField("T", "string", "s", true, 4)},
			data:   map[string]interface{}{"s": []string{"new\nline", "more \"quotes\"", "escape a s\\ash", "t\tab"}},
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\n expected: %v\nerr: %v", testCase.data, err)
		}

		defaultMarshalled, err := json.Marshal(testCase.data)
		if err != nil {
			t.Fatalf("failed to marshal test case data\n expected: %v\nerr: %v", testCase.data, err)
		}

		if string(marshalled) != string(defaultMarshalled) {
			t.Fatalf("marshalled data does not equal expected\nmarshalled: %v\nexpected: %v", string(marshalled), string(defaultMarshalled))
		}
	}
}

// Ensure that we will still unmarshal a u8 array of numbers (even though the default representation is base64).
func TestDynamicMessage_marshalJSON_u8Array(t *testing.T) {

	testCases := []struct {
		fields     []gengo.Field
		marshalled string
		data       []uint8
	}{
		{
			fields:     []gengo.Field{*gengo.NewField("T", "uint8", "u8", true, -1)},
			marshalled: `{"u8":[0,1,2,3,4,5]}`,
			data:       []uint8{0, 1, 2, 3, 4, 5},
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        make(map[string]interface{}),
		}

		if err := json.Unmarshal([]byte(testCase.marshalled), testMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}

		if reflect.DeepEqual(testCase.data, testMessage.data["u8"].([]uint8)) == false {
			t.Fatalf("unmarshalled data did not match expected\n unmarshalled: %v\n expected: %v", testCase.data, testMessage.data["u8"].([]uint8))
		}
	}
}

func TestDynamicMessage_marshalTimeFormats(t *testing.T) {

	testCases := []struct {
		fields     []gengo.Field
		marshalled string
		data       map[string]interface{}
	}{
		{
			// Standard roscpp Time format
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "stamp", false, -1)},
			marshalled: `{"stamp":{"sec":1614894959,"nsec":184787297}}`,
			data:       map[string]interface{}{"stamp": NewTime(1614894959, 184787297)},
		},
		{
			// rospy Time Format
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "stamp", false, -1)},
			marshalled: `{"stamp":{"secs":1614894959,"nsecs":184787297}}`,
			data:       map[string]interface{}{"stamp": NewTime(1614894959, 184787297)},
		},
		{
			// Old Rocos Time Format
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "stamp", false, -1)},
			marshalled: `{"stamp":{"Sec":1614894959,"NSec":184787297}}`,
			data:       map[string]interface{}{"stamp": NewTime(1614894959, 184787297)},
		},
		{
			// Old Rocos Time Format Plural
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "stamp", false, -1)},
			marshalled: `{"stamp":{"Secs":1614894959,"NSecs":184787297}}`,
			data:       map[string]interface{}{"stamp": NewTime(1614894959, 184787297)},
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\nerr: %v", err)
		}

		// Test unmarshalling using two different sets of bytes:
		// 1) the marshalled bytes above.
		// 2) the predefined bytes string in testCase

		// 1) Unmarshal using the marshalled bytes.
		unmarshalledMessage := testMessageType.NewDynamicMessage()
		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", string(marshalled), err)
		}
		if reflect.DeepEqual(testCase.data, unmarshalledMessage.data) == false {
			t.Fatalf("unmarshalled data did not match expected\n unmarshalled: %v\n expected: %v", unmarshalledMessage.data, testMessage.data)
		}

		// 2) Now use the test case marshalled string.
		unmarshalledMessage = testMessageType.NewDynamicMessage()
		if err := json.Unmarshal([]byte(testCase.marshalled), unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}
		if reflect.DeepEqual(testCase.data, unmarshalledMessage.data) == false {
			t.Fatalf("unmarshalled data did not match expected\n unmarshalled: %v\n expected: %v", unmarshalledMessage.data, testMessage.data)
		}
	}
}

func TestDynamicMessage_marshalDurationFormats(t *testing.T) {

	testCases := []struct {
		fields     []gengo.Field
		marshalled string
		data       map[string]interface{}
	}{
		{
			// Standard roscpp Duration format
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, -1)},
			marshalled: `{"d":{"sec":1614894959,"nsec":184787297}}`,
			data:       map[string]interface{}{"d": NewDuration(1614894959, 184787297)},
		},
		{
			// rospy Duration Format
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, -1)},
			marshalled: `{"d":{"secs":1614894959,"nsecs":184787297}}`,
			data:       map[string]interface{}{"d": NewDuration(1614894959, 184787297)},
		},
		{
			// Old Rocos Duration Format
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, -1)},
			marshalled: `{"d":{"Sec":1614894959,"NSec":184787297}}`,
			data:       map[string]interface{}{"d": NewDuration(1614894959, 184787297)},
		},
		{
			// Old Rocos Duration Format Plural
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, -1)},
			marshalled: `{"d":{"Secs":1614894959,"NSecs":184787297}}`,
			data:       map[string]interface{}{"d": NewDuration(1614894959, 184787297)},
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\nerr: %v", err)
		}

		// Test unmarshalling using two different sets of bytes:
		// 1) the marshalled bytes above.
		// 2) the predefined bytes string in testCase

		// 1) Unmarshal using the marshalled bytes.
		unmarshalledMessage := testMessageType.NewDynamicMessage()
		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", string(marshalled), err)
		}
		if reflect.DeepEqual(testCase.data, unmarshalledMessage.data) == false {
			t.Fatalf("unmarshalled data did not match expected\n unmarshalled: %v\n expected: %v", unmarshalledMessage.data, testMessage.data)
		}

		// 2) Now use the test case marshalled string.
		unmarshalledMessage = testMessageType.NewDynamicMessage()
		if err := json.Unmarshal([]byte(testCase.marshalled), unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}
		if reflect.DeepEqual(testCase.data, unmarshalledMessage.data) == false {
			t.Fatalf("unmarshalled data did not match expected\n unmarshalled: %v\n expected: %v", unmarshalledMessage.data, testMessage.data)
		}
	}
}

// TestDynamicMessage_marshalHeader tests the cpp and python header formats.
func TestDynamicMessage_marshalHeader(t *testing.T) {
	headerType, err := NewDynamicMessageType("Header")
	if err != nil {
		t.Skip("test skipped because ROS environment not set up")
		return
	}

	testCases := []struct {
		name       string
		frame_id   string
		seq        uint32
		stamp      Time
		marshalled string
	}{
		{
			name:       "standard_c++_format",
			frame_id:   "map",
			seq:        0,
			stamp:      NewTime(1614894959, 184787297),
			marshalled: `{"frame_id":"map","seq":0,"stamp":{"sec":1614894959,"nsec":184787297}}`,
		},
		{
			name:       "python_format",
			frame_id:   "map",
			seq:        0,
			stamp:      NewTime(1614894959, 184787297),
			marshalled: `{"frame_id":"map","seq":0,"stamp":{"secs":1614894959,"nsecs":184787297}}`,
		},
		{
			name:       "old_rocos_format",
			frame_id:   "map",
			seq:        0,
			stamp:      NewTime(1614894959, 184787297),
			marshalled: `{"frame_id":"map","seq":0,"stamp":{"Sec":1614894959,"NSec":184787297}}`,
		},
		{
			name:       "old_rocos_format_plural",
			frame_id:   "map",
			seq:        0,
			stamp:      NewTime(1614894959, 184787297),
			marshalled: `{"frame_id":"map","seq":0,"stamp":{"Secs":1614894959,"NSecs":184787297}}`,
		},
	}

	for _, testCase := range testCases {

		testMessage := headerType.NewDynamicMessage()
		testMessage.data["frame_id"] = testCase.frame_id
		testMessage.data["seq"] = testCase.seq
		testMessage.data["stamp"] = testCase.stamp

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\n expected: %v\nerr: %v", testCase.marshalled, err)
		}

		// Test unmarshalling using two different sets of bytes:
		// 1) the marshalled bytes above.
		// 2) the predefined bytes string in testCase

		// 1) Unmarshal using the marshalled bytes.
		unmarshalledMessage := headerType.NewDynamicMessage()
		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", string(marshalled), err)
		}
		for key := range testMessage.data {
			original := testMessage.data[key]
			unmarshalled := unmarshalledMessage.data[key]
			if reflect.DeepEqual(original, unmarshalled) == false {
				t.Fatalf("original and unmarshalled data mismatch for test case %s. \n Original: %v \n Unmarshalled: %v \n json: %v", testCase.name, testMessage.data, unmarshalledMessage.data, string(marshalled))
			}
		}

		// 2) Now use the test case marshalled string to confirm the different formats are correctly unmarshalled.
		unmarshalledMessage = headerType.NewDynamicMessage()
		if err := json.Unmarshal([]byte(testCase.marshalled), unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}
		for key := range testMessage.data {
			original := testMessage.data[key]
			unmarshalled := unmarshalledMessage.data[key]
			if reflect.DeepEqual(original, unmarshalled) == false {
				t.Fatalf("original and unmarshalled data mismatch for test case %s. \n Original: %v \n Unmarshalled: %v \n json: %v", testCase.name, testMessage.data, unmarshalledMessage.data, string(testCase.marshalled))
			}
		}
	}
}

// Verify the NaN behavior of floats. This cannot be done using reflect deep-equal, so gets its own test case.
func TestDynamicMessage_JSON_floatNan(t *testing.T) {

	testCases := []struct {
		fields     []gengo.Field
		data       map[string]interface{}
		marshalled string
	}{
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float32", "f", false, 0)},
			data:       map[string]interface{}{"f": JsonFloat32{F: float32(math.NaN())}},
			marshalled: `{"f":"nan"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float64", "f", false, 0)},
			data:       map[string]interface{}{"f": JsonFloat64{F: math.NaN()}},
			marshalled: `{"f":"nan"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float32", "f", true, 2)},
			data:       map[string]interface{}{"f": []JsonFloat32{{float32(math.NaN())}, {float32(math.NaN())}}},
			marshalled: `{"f":["nan","nan"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float64", "f", true, 2)},
			data:       map[string]interface{}{"f": []JsonFloat64{{math.NaN()}, {math.NaN()}}},
			marshalled: `{"f":["nan","nan"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float32", "f", true, -1)},
			data:       map[string]interface{}{"f": []JsonFloat32{{float32(math.NaN())}, {float32(math.NaN())}}},
			marshalled: `{"f":["nan","nan"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("T", "float64", "f", true, -1)},
			data:       map[string]interface{}{"f": []JsonFloat64{{math.NaN()}, {math.NaN()}}},
			marshalled: `{"f":["nan","nan"]}`,
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\n expected: %v\nerr: %v", testCase.marshalled, err)
		}
		if string(marshalled) != testCase.marshalled {
			t.Fatalf("marshalled data does not equal expected\nmarshalled: %v\nexpected: %v", string(marshalled), testCase.marshalled)
		}

		unmarshalledMessage := testMessageType.NewDynamicMessage()

		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}

		if testCase.fields[0].IsArray {
			if testCase.fields[0].GoType == "float32" {
				for _, val := range unmarshalledMessage.data["f"].([]JsonFloat32) {
					if math.IsNaN(float64(val.F)) == false {
						t.Fatalf("%v: unmarshalled value was not NaN, actual: %v", testCase.marshalled, val.F)
					}
				}
			} else {
				for _, val := range unmarshalledMessage.data["f"].([]JsonFloat64) {
					if math.IsNaN(val.F) == false {
						t.Fatalf("%v: unmarshalled value was not NaN, actual: %v", testCase.marshalled, val.F)
					}
				}
			}
		} else {
			var val float64
			if testCase.fields[0].GoType == "float32" {
				val = float64(unmarshalledMessage.data["f"].(JsonFloat32).F)
			} else {
				val = unmarshalledMessage.data["f"].(JsonFloat64).F
			}
			if math.IsNaN(val) == false {
				t.Fatalf("%v: unmarshalled value was not NaN, actual: %v", testCase.marshalled, val)
			}
		}

	}
}

func TestDynamicMessage_marshalJSON_nested(t *testing.T) {
	poseType, err := NewDynamicMessageType("geometry_msgs/Pose")
	if err != nil {
		t.Skip("test skipped because ROS environment not set up")
		return
	}

	testCases := []struct {
		position    map[string]interface{}
		orientation map[string]interface{}
		marshalled  string
	}{
		{
			position:    map[string]interface{}{"x": JsonFloat64{0}, "y": JsonFloat64{0}, "z": JsonFloat64{0}},
			orientation: map[string]interface{}{"x": JsonFloat64{0}, "y": JsonFloat64{0}, "z": JsonFloat64{0}, "w": JsonFloat64{0}},
			marshalled:  `{"position":{"x":0,"y":0,"z":0},"orientation":{"x":0,"y":0,"z":0,"w":0}}`,
		},
		{
			position:    map[string]interface{}{"x": JsonFloat64{1e9}, "y": JsonFloat64{2.4e-2}, "z": JsonFloat64{5.5e3}},
			orientation: map[string]interface{}{"x": JsonFloat64{-0.5}, "y": JsonFloat64{0.5}, "z": JsonFloat64{0.5}, "w": JsonFloat64{0.5}},
			marshalled:  `{"position":{"x":1e+09,"y":0.024,"z":5500},"orientation":{"x":-0.5,"y":0.5,"z":0.5,"w":0.5}}`,
		},
	}

	for _, testCase := range testCases {

		testMessage := poseType.NewDynamicMessage()
		testMessage.data["position"].(*DynamicMessage).data = testCase.position
		testMessage.data["orientation"].(*DynamicMessage).data = testCase.orientation

		marshalled, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal dynamic message\n expected: %v\nerr: %v", testCase.marshalled, err)
		}
		if string(marshalled) != testCase.marshalled {
			t.Fatalf("marshalled data does not equal expected\nmarshalled: %v\nexpected: %v", string(marshalled), testCase.marshalled)
		}

		unmarshalledMessage := poseType.NewDynamicMessage()

		if err := json.Unmarshal(marshalled, unmarshalledMessage); err != nil {
			t.Fatalf("failed to unmarshal dynamic message\n json: %v\nerr: %v", testCase.marshalled, err)
		}

		for key := range testMessage.data {
			original := testMessage.data[key].(*DynamicMessage).data
			unmarshalled := unmarshalledMessage.data[key].(*DynamicMessage).data
			if reflect.DeepEqual(original, unmarshalled) == false {
				t.Fatalf("original and unmarshalled data mismatch. \n Original: %v \n Unmarshalled: %v \n json: %v", testMessage.data, unmarshalledMessage.data, string(marshalled))
			}
		}
	}
}

func TestDynamicMessage_marshalJSON_nestedWithTypeError(t *testing.T) {
	testMessageType, err := NewDynamicMessageType("geometry_msgs/Pose")

	if err != nil {
		t.Skip("test skipped because ROS environment not set up")
		return
	}

	testMessage := testMessageType.NewDynamicMessage()
	testMessage.data["position"] = float64(543.21) // We expect this to be geometry_msgs/Point type.

	if _, err := json.Marshal(testMessage); err == nil {
		t.Fatalf("expected type error")
	}
}

func TestDynamicMessage_marshalJSON_arrayOfNestedMessages(t *testing.T) {
	// We don't care about Pose in this step, but we want to load libgengo's context.
	_, err := NewDynamicMessageType("geometry_msgs/Pose")

	if err != nil {
		t.Skip("test skipped because ROS environment not set up")
		return
	}
	// Structure is z->[x, x].
	fields := []gengo.Field{
		*gengo.NewField("test", "uint8", "val", false, 0),
	}
	msgSpec := generateTestSpec(fields)
	msgSpec.FullName = "test/x0Message"
	context.RegisterMsg("test/x0Message", msgSpec)

	fields = []gengo.Field{
		*gengo.NewField("test", "x0Message", "x", true, 2),
	}
	msgSpec = generateTestSpec(fields)
	msgSpec.FullName = "test/z0Message"
	context.RegisterMsg("test/z0Message", msgSpec)

	testMessageType, err := NewDynamicMessageType("test/z0Message")
	if err != nil {
		t.Fatalf("Failed to create testMessageType, error: %v", err)
	}

	expectedMarshalled := `{"x":[{"val":0},{"val":0}]}`

	testMessage := testMessageType.NewDynamicMessage()

	marshalledBytes, err := json.Marshal(testMessage)
	if err != nil {
		t.Fatalf("failed to marshal dynamic message, err: %v, msg: %v", err, testMessage.data)
	}
	if string(marshalledBytes) != expectedMarshalled {
		t.Fatalf("marshalled does not match expected, actual: %v, expected: %v", string(marshalledBytes), expectedMarshalled)
	}

	unmarshalledMessage := testMessage.dynamicType.NewDynamicMessage()
	err = json.Unmarshal(marshalledBytes, unmarshalledMessage)
	if err != nil {
		t.Fatalf("failed to unmarshal dynamic message, %v", err)
	}

	if len(testMessage.data["x"].([]Message)) != len(unmarshalledMessage.data["x"].([]Message)) {
		t.Fatalf("original and custom marshal mismatch \n original: %v \n custom: %v", testMessage.data, unmarshalledMessage.data)
	}

	// Extra check: ensure type error is handled.
	testMessage.data["x"] = []float64{543.21, 98.76} // We expect this to be xMessage array.

	if _, err := json.Marshal(testMessage); err == nil {
		t.Fatalf("expected type error")
	}
}

// Ensure that invalid marshalling results in errors and not panic
func TestDynamicMessage_JSONMarshal_typeErrors(t *testing.T) {

	testCases := []struct {
		fields []gengo.Field
		data   map[string]interface{}
	}{
		// Singular Values.
		// - Unsigned integers.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)},
			data:   map[string]interface{}{"u8": uint16(0x12)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", false, 0)},
			data:   map[string]interface{}{"u16": uint8(0x80)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", false, 0)},
			data:   map[string]interface{}{"u32": int32(0x40000001)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", false, 0)},
			data:   map[string]interface{}{"u64": NewTime(0, 0)},
		},
		// - Signed integers.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int8", "i8", false, 0)},
			data:   map[string]interface{}{"i8": float32(-20)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int16", "i16", false, 0)},
			data:   map[string]interface{}{"i16": uint16(20_000)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int32", "i32", false, 0)},
			data:   map[string]interface{}{"i32": int8(-2)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int64", "i64", false, 0)},
			data:   map[string]interface{}{"i64": float64(-2_000_000_000)},
		},
		// - Booleans.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			data:   map[string]interface{}{"b": 0},
		},
		// - Floats.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			data:   map[string]interface{}{"f32": JsonFloat64{0.0}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:   map[string]interface{}{"f64": float32(-1.125)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			data:   map[string]interface{}{"f64": float64(-1.125)},
		},
		// - Strings.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			data:   map[string]interface{}{"s": []byte{0x64, 0x65}},
		},
		// - Time and Duration.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			data:   map[string]interface{}{"t": NewDuration(0xfeedf00d, 0x1337beef)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, 0)},
			data:   map[string]interface{}{"d": uint64(0x40302010)},
		},
		// - Message (not builtin)
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "customType", "m", false, 0)},
			data:   map[string]interface{}{"m": 0},
		},
		// Fixed and Dynamic arrays.
		// - Unsigned integers.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", true, 8)},
			data:   map[string]interface{}{"u8": []uint16{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, 5)},
			data:   map[string]interface{}{"u16": []uint32{0xf0de, 0xbc9a, 0x7856, 0x3412, 0x0}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, 3)},
			data:   map[string]interface{}{"u32": []uint64{0xf0debc9a, 0x78563412, 0x0}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, 3)},
			data:   map[string]interface{}{"u64": []int64{0x8000000000001, 0x78563412, 0x0}},
		},
		// - Signed integers.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int8", "i8", true, 8)},
			data:   map[string]interface{}{"i8": []int16{-128, -55, -1, 0, 1, 7, 77, 127}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, 7)},
			data:   map[string]interface{}{"i16": []int32{-32768, -129, -1, 0, 1, 128, 32767}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, 9)},
			data:   map[string]interface{}{"i32": []int64{-2147483648, -32768, -129, -1, 0, 1, 128, 32767, 2147483647}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, 1)},
			data:   map[string]interface{}{"i64": int64(0)},
		},
		// - Booleans.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 2)},
			data:   map[string]interface{}{"b": []int{0, 1}},
		},
		// - Floats.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "float32", "f32", true, 4)},
			data:   map[string]interface{}{"f32": []JsonFloat64{{-1.125}, {3.3e3}, {7.7e7}, {9.9e-9}}},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "float64", "f64", true, 1)},
			data:   map[string]interface{}{"f64": JsonFloat64{-1.125}},
		},
		// - Strings.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "string", "s", true, 6)},
			data:   map[string]interface{}{"s": "string"},
		},
		// - Time and Duration.
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "time", "t", true, 1)},
			data:   map[string]interface{}{"t": NewTime(0xfeedf00d, 0x1337beef)},
		},
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 1)},
			data:   map[string]interface{}{"d": NewDuration(0xfeedf00d, 0x1337beef)},
		},
		// - Messages (not builtin).
		{
			fields: []gengo.Field{*gengo.NewField("Testing", "customType", "m", true, 1)},
			data:   map[string]interface{}{"m": []int{0}},
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := &DynamicMessage{
			dynamicType: testMessageType,
			data:        testCase.data,
		}

		marshalled, err := json.Marshal(testMessage)
		if err == nil {
			t.Fatalf("marshalled invalid dynamic message data\njson: %v", marshalled)
		}
	}
}

func TestDynamicMessage_JSONUnmarshal_errors(t *testing.T) {
	testCases := []struct {
		fields     []gengo.Field
		marshalled string
	}{
		// Singular Values.
		// - Unsigned integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)},
			marshalled: `{"u8":"18"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", false, 0)},
			marshalled: `{"u16":10.5}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", false, 0)},
			marshalled: `{"u32":false}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", false, 0)},
			marshalled: `{"u64":{"sec":0,"nsec":1}}`,
		},
		// - Signed integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int8", "i8", false, 0)},
			marshalled: `{"i8":[0,1,2,3,4]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", false, 0)},
			marshalled: `{"i16":{}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", false, 0)},
			marshalled: `{"i32":""}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", false, 0)},
			marshalled: `{"i64":12.34}`,
		},
		// - Booleans.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			marshalled: `{"b":0}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			marshalled: `{"b":1.2}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			marshalled: `{"b":{}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", false, 0)},
			marshalled: `{"b":[true]}`,
		},
		// - Floats.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":"nAAn"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":1e}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":-2.3e^08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":[1.2]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":"+infff"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", false, 0)},
			marshalled: `{"f32":{}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":"nAAn"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":1e}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":-2.3e^08}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":[1.2]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":"+infff"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", false, 0)},
			marshalled: `{"f64":{}}`,
		},
		// - Strings.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			marshalled: `{"s":no quotes}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			marshalled: `{"s":\"escaped quotes\"}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			marshalled: `{"s":0}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			marshalled: `{"s":["arr"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", false, 0)},
			marshalled: `{"s":{"obj":"val"}}`,
		},
		// - Time and Duration.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			marshalled: `{"t":{}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			marshalled: `{"t":{"nsec":1}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			marshalled: `{"t":{"sec":1}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", false, 0)},
			marshalled: `{"t":1.5}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, 0)},
			marshalled: `{"d":{"sec":true,"nsec":true}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, 0)},
			marshalled: `{"d":[{"sec":"","nsec":""}]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", false, 0)},
			marshalled: `{"d":false}`,
		},
		// Arrays.
		// - Unsigned integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", true, 1)},
			marshalled: `{"u8":"001"}`, // Invalid base64.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint8", "u8", true, 1)},
			marshalled: `{"u8":["AAAAA"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, 5)},
			marshalled: `{"u16":[61662,48282,30806,"13330",0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, 5)},
			marshalled: `{"u16":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint16", "u16", true, 1)},
			marshalled: `{"u16":30806}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, 5)},
			marshalled: `{"u32":[61662,48282,30806,{"v":"13330"},0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, 5)},
			marshalled: `{"u32":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint32", "u32", true, 1)},
			marshalled: `{"u32":30806}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, 5)},
			marshalled: `{"u64":[61662,48282,30806,[13330],0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, 5)},
			marshalled: `{"u64":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "uint64", "u64", true, 1)},
			marshalled: `{"u64":30806}`,
		},
		// - Signed integers.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int8", "i8", true, 5)},
			marshalled: `{"i8":[61,48,30,"13",0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i8", true, 5)},
			marshalled: `{"i8":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i8", true, 1)},
			marshalled: `{"i8":6}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, 5)},
			marshalled: `{"i16":[61662,48282,30806,"13330",0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, 5)},
			marshalled: `{"i16":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int16", "i16", true, 1)},
			marshalled: `{"i16":30806}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, 5)},
			marshalled: `{"i32":[61662,48282,30806,{"v":13330},0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, 5)},
			marshalled: `{"i32":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int32", "i32", true, 1)},
			marshalled: `{"i32":30806}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, 5)},
			marshalled: `{"i64":[61662,48282,30806,[13330],0]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, 5)},
			marshalled: `{"i64":[0]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "int64", "i64", true, 1)},
			marshalled: `{"i64":30806}`,
		},
		// - Booleans.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 1)},
			marshalled: `{"b":true}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 1)},
			marshalled: `{"b":[true,false]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 2)},
			marshalled: `{"b":[1,2]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "bool", "b", true, 1)},
			marshalled: `{"b":["true"]}`,
		},
		// - Floats.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", true, 4)},
			marshalled: `{"f32":[-1.125,3300,7.7e+07,9.9e-09,"+inf","-inf"]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float32", "f32", true, 1)},
			marshalled: `{"f32":["zero"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", true, 1)},
			marshalled: `{"f64":-1.125}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "float64", "f64", true, 1)},
			marshalled: `{"f64":[{1.2}]}`,
		},
		// - Strings.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", true, 5)},
			marshalled: `{"s":[""]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", true, 2)},
			marshalled: `{"s":[0,"1"]}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "string", "s", true, -1)},
			marshalled: `{"s":"string"}`,
		},
		// - Time.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", true, 2)},
			marshalled: `{"t":[{"sec":322420463,"nsec":7364688}]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", true, 1)},
			marshalled: `{"t":{"sec":322420463,"nsec":7364688}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "time", "t", true, 1)},
			marshalled: `{"t":[{"sec":"zero","nsec":7364688}]}`,
		},
		// - Duration.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 2)},
			marshalled: `{"d":[{"sec":322420463,"nsec":7364688}]}`, // Wrong length.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 2)},
			marshalled: `{"d":{"sec":322420463,"nsec":7364688}}`,
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 2)},
			marshalled: `{"d":[{"sec":"zero","nsec":7364688}]}`,
		},
		// Other errors.
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 1)},
			marshalled: `{"t":[{"sec":0,"nsec":7364688}]}`, // Key not found.
		},
		{
			fields:     []gengo.Field{*gengo.NewField("Testing", "duration", "d", true, 1)},
			marshalled: `{"d":[{"sec":0,"nsec":7364688}],"t":[{"sec":0,"nsec":7364688}]}`, // Unknown key.
		},
	}

	for _, testCase := range testCases {

		testMessageType := &DynamicMessageType{
			spec:         generateTestSpec(testCase.fields),
			nested:       make(map[string]*DynamicMessageType),
			jsonPrealloc: 0,
		}

		testMessage := testMessageType.NewDynamicMessage()

		if err := json.Unmarshal([]byte(testCase.marshalled), testMessage); err == nil {
			t.Fatalf("unmarshalled invalid dynamic message json\njson: %v", testCase.marshalled)
		}
	}
}

func TestDynamicMessage_marshalJSON_invalidPointers(t *testing.T) {

	marshalled := []byte(`{"u8":0}`)
	testMessageType := &DynamicMessageType{
		spec:         generateTestSpec([]gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}
	testMessage := testMessageType.NewDynamicMessage()

	// Nil pointer to dynamic message type
	testMessage.dynamicType = nil
	if _, err := testMessage.MarshalJSON(); err == nil {
		t.Fatal("expected error when marshalling nil type")
	}
	if err := testMessage.UnmarshalJSON(marshalled); err == nil {
		t.Fatal("expected error when unmarshalling nil type")
	}

	// Nil pointer to dynamic message data
	testMessage = testMessageType.NewDynamicMessage()
	testMessage.data = nil
	if _, err := testMessage.MarshalJSON(); err == nil {
		t.Fatal("expected error when marshalling nil data")
	}
	if err := testMessage.UnmarshalJSON(marshalled); err == nil {
		t.Fatal("expected error when unmarshalling nil data")
	}

	// Nil pointer to dynamic message nested types
	testMessage = testMessageType.NewDynamicMessage()
	testMessage.dynamicType.nested = nil
	if _, err := testMessage.MarshalJSON(); err == nil {
		t.Fatal("expected error when marshalling nil nested")
	}
	if err := testMessage.UnmarshalJSON(marshalled); err == nil {
		t.Fatal("expected error when unmarshalling nil nested")
	}

	testMessageType = &DynamicMessageType{
		spec:         generateTestSpec([]gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	// Nil pointer to dynamic message spec
	testMessage = testMessageType.NewDynamicMessage()
	testMessage.dynamicType.spec = nil
	if _, err := testMessage.MarshalJSON(); err == nil {
		t.Fatal("expected error when marshalling nil spec")
	}
	if err := testMessage.UnmarshalJSON(marshalled); err == nil {
		t.Fatal("expected error when unmarshalling nil spec")
	}

	testMessageType = &DynamicMessageType{
		spec:         generateTestSpec([]gengo.Field{*gengo.NewField("Testing", "uint8", "u8", false, 0)}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	// Nil pointer to dynamic message spec fields
	testMessage = testMessageType.NewDynamicMessage()
	testMessage.dynamicType.spec.Fields = nil
	if _, err := testMessage.MarshalJSON(); err == nil {
		t.Fatal("expected error when marshalling nil spec fields")
	}
	if err := testMessage.UnmarshalJSON(marshalled); err == nil {
		t.Fatal("expected error when unmarshalling nil spec fields")
	}

}
