package ros

import (
	"encoding/json"
	"testing"

	gengo "github.com/team-rocos/rosgo/libgengo"
)

// Benchmarks on JSON marshalling.

var singularMessageData map[string]interface{} = map[string]interface{}{
	"u8":  uint8(0x12),
	"u16": uint16(0x3456),
	"u32": uint32(0x789abcde),
	"u64": uint64(0x123456789abcdef0),
	"i8":  int8(-2),
	"i16": int16(-2),
	"i32": int32(-2),
	"i64": int64(-2),
	"b":   true,
	"f32": JsonFloat32{1234.5678},
	"f64": JsonFloat64{-9876.5432},
	"s":   "Rocos",
	"t":   NewTime(0xfeedf00d, 0x1337beef),
	"d":   NewDuration(0x50607080, 0x10203040),
}

var fixedArrayMessageData = map[string]interface{}{
	"u8":  []uint8{0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	"u16": []uint16{0xdef0, 0x9abc, 0x5678, 0x1234},
	"u32": []uint32{0x9abcdef0, 0x12345678},
	"u64": []uint64{0x123456789abcdef0},
	"i8":  []int8{-2, -1, 0, 1, 2, 3, 4, 5},
	"i16": []int16{-2, -1, 0, 1},
	"i32": []int32{-2, 1},
	"i64": []int64{-2},
	"b":   []bool{true, true, false, false, true, false, true, false},
	"f32": []JsonFloat32{{1234.5678}, {1234.5678}},
	"f64": []JsonFloat64{{-9876.5432}},
	"s":   []string{"Rocos", "soroc", "croos"},
	"t":   []Time{NewTime(0xfeedf00d, 0x1337beef), NewTime(0x1337beef, 0x1337f00d)},
	"d":   []Duration{NewDuration(0x40302010, 0x00706050), NewDuration(0x50607080, 0x10203040)},
}

type MockDefaultDynamicMessage struct {
	data map[string]interface{}
}

func (m MockDefaultDynamicMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.data)
}

func (m MockDefaultDynamicMessage) UnmarshalJSON(buf []byte) error {
	return nil
}

func BenchmarkDynamicMessage_JSONMarshal_SingularPrimitives_default(b *testing.B) {
	m := MockDefaultDynamicMessage{data: singularMessageData}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_SingularPrimitives_custom(b *testing.B) {
	testMessage := singularMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	testMessage.data = singularMessageData

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_SingularPrimitives_customNoLibs(b *testing.B) {
	testMessage := singularMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	testMessage.data = singularMessageData

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONUnmarshal_SingularPrimitives(b *testing.B) {
	originalMessage := singularMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	originalMessage.data = singularMessageData

	marshalled, err := json.Marshal(originalMessage)
	if err != nil {
		b.Fatalf("marshal failed %s", err)
	}

	testMessage := singularMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := json.Unmarshal(marshalled, testMessage)
		if err != nil {
			b.Fatalf("unmarshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_FixedArrays_default(b *testing.B) {
	m := MockDefaultDynamicMessage{fixedArrayMessageData}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_FixedArrays_custom(b *testing.B) {
	testMessage := fixedArrayMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	testMessage.data = fixedArrayMessageData

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_FixedArrays_customNoLib(b *testing.B) {
	testMessage := fixedArrayMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	testMessage.data = fixedArrayMessageData

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONUnmarshal_FixedArrays(b *testing.B) {
	originalMessage := fixedArrayMessageType.NewDynamicMessage() // from dynamic_message_benchmark_test.go
	originalMessage.data = fixedArrayMessageData

	marshalled, err := json.Marshal(originalMessage)
	if err != nil {
		b.Fatalf("marshal failed %s", err)
	}

	testMessage := fixedArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := json.Unmarshal(marshalled, testMessage)
		if err != nil {
			b.Fatalf("unmarshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_Nested(b *testing.B) {
	testMessageType, err := NewDynamicMessageType("geometry_msgs/Pose")
	if err != nil {
		b.Skip("test skipped because ROS environment not set up")
		return
	}
	testMessage := testMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		// _, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONUnmarshal_Nested(b *testing.B) {
	testMessageType, err := NewDynamicMessageType("geometry_msgs/Pose")
	if err != nil {
		b.Skip("test skipped because ROS environment not set up")
		return
	}
	originalMessage := testMessageType.NewDynamicMessage()

	marshalled, err := json.Marshal(originalMessage)
	if err != nil {
		b.Fatalf("marshal failed %s", err)
	}

	testMessage := testMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := json.Unmarshal(marshalled, testMessage)
		if err != nil {
			b.Fatalf("unmarshal failed %s", err)
		}
	}
}

// Benchmark marshalling arrays.

var arrayLength int = 1000000

var boolArrayMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "bool", "b", true, arrayLength),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayBool_default(b *testing.B) {
	testMessage := boolArrayMessageType.NewDynamicMessage()
	m := MockDefaultDynamicMessage{testMessage.data}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}
func BenchmarkDynamicMessage_JSONMarshal_ArrayBool_custom(b *testing.B) {
	testMessage := boolArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayBool_customNoLib(b *testing.B) {
	testMessage := boolArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

var uint8ArrayMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", true, arrayLength),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayUint8_default(b *testing.B) {
	testMessage := uint8ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u8"].([]uint8) {
		testMessage.data["u8"].([]uint8)[i] = 0x5a
	}
	m := MockDefaultDynamicMessage{testMessage.data}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}
func BenchmarkDynamicMessage_JSONMarshal_ArrayUint8_custom(b *testing.B) {
	testMessage := uint8ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u8"].([]uint8) {
		testMessage.data["u8"].([]uint8)[i] = 0x5a
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayUint8_customNoLib(b *testing.B) {
	testMessage := uint8ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u8"].([]uint8) {
		testMessage.data["u8"].([]uint8)[i] = 0x5a
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

// The encoding time for u16, u32, u64, i8, i16, i32 and i64 are all expected to be equivalent.
var uint64ArrayMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "uint64", "u64", true, arrayLength),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayUint64_default(b *testing.B) {
	testMessage := uint64ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u64"].([]uint64) {
		testMessage.data["u64"].([]uint64)[i] = 0x5a
	}
	m := MockDefaultDynamicMessage{testMessage.data}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}
func BenchmarkDynamicMessage_JSONMarshal_ArrayUint64_custom(b *testing.B) {
	testMessage := uint64ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u64"].([]uint64) {
		testMessage.data["u64"].([]uint64)[i] = 0x5a
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(testMessage)
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONMarshal_ArrayUint64_customNoLib(b *testing.B) {
	testMessage := uint64ArrayMessageType.NewDynamicMessage()
	for i := range testMessage.data["u64"].([]uint64) {
		testMessage.data["u64"].([]uint64)[i] = 0x5a
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := testMessage.MarshalJSON()
		if err != nil {
			b.Fatalf("marshal failed %s", err)
		}
	}
}

func BenchmarkDynamicMessage_JSONUnmarshal_bigArray(b *testing.B) {
	var testMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint16", "u16", true, 500_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}
	originalMessage := testMessageType.NewDynamicMessage()
	for i := range originalMessage.data["u16"].([]uint16) {
		originalMessage.data["u16"].([]uint16)[i] = 0x5a31
	}

	marshalled, err := json.Marshal(originalMessage)
	if err != nil {
		b.Fatalf("marshal failed %s", err)
	}

	testMessage := testMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := json.Unmarshal(marshalled, testMessage)
		if err != nil {
			b.Fatalf("unmarshal failed %s", err)
		}
	}
}
