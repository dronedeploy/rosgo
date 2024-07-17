package ros

import (
	"bytes"
	"testing"

	gengo "github.com/team-rocos/rosgo/libgengo"
)

// Benchmarks on primitive ROS data types.

// Singular value type used for testing across all ROS primitives.
var singularMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", false, 0),
		*gengo.NewField("Testing", "uint16", "u16", false, 0),
		*gengo.NewField("Testing", "uint32", "u32", false, 0),
		*gengo.NewField("Testing", "uint64", "u64", false, 0),
		*gengo.NewField("Testing", "int8", "i8", false, 0),
		*gengo.NewField("Testing", "int16", "i16", false, 0),
		*gengo.NewField("Testing", "int32", "i32", false, 0),
		*gengo.NewField("Testing", "int64", "i64", false, 0),
		*gengo.NewField("Testing", "bool", "b", false, 0),
		*gengo.NewField("Testing", "float32", "f32", false, 0),
		*gengo.NewField("Testing", "float64", "f64", false, 0),
		*gengo.NewField("Testing", "string", "s", false, 0),
		*gengo.NewField("Testing", "time", "t", false, 0),
		*gengo.NewField("Testing", "duration", "d", false, 0),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

var singularSerialized []byte = []byte{
	0x12,       // u8
	0x56, 0x34, // u16
	0xde, 0xbc, 0x9a, 0x78, // u32
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u64
	0xfe,       // i8
	0xfe, 0xff, // i16
	0xfe, 0xff, 0xff, 0xff, // i32
	0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // i64
	0x01,                   // b
	0x2b, 0x52, 0x9a, 0x44, // f32
	0x98, 0xdd, 0x93, 0x87, 0x45, 0x4a, 0xc3, 0xc0, // f64
	0x05, 0x00, 0x00, 0x00, 'R', 'o', 'c', 'o', 's', // s
	0x0d, 0xf0, 0xed, 0xfe, 0xef, 0xbe, 0x37, 0x13, // t
	0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, // d
}

// Benchmark deserializing across all primitives as singular entries to the message type.
func BenchmarkDynamicMessage_Deserialize_SingularPrimitives(b *testing.B) {

	testMessage := singularMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(singularSerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Fixed array type used for testing across all ROS primitives.
var fixedArrayMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", true, 8),
		*gengo.NewField("Testing", "uint16", "u16", true, 4),
		*gengo.NewField("Testing", "uint32", "u32", true, 2),
		*gengo.NewField("Testing", "uint64", "u64", true, 1),
		*gengo.NewField("Testing", "int8", "i8", true, 8),
		*gengo.NewField("Testing", "int16", "i16", true, 4),
		*gengo.NewField("Testing", "int32", "i32", true, 2),
		*gengo.NewField("Testing", "int64", "i64", true, 1),
		*gengo.NewField("Testing", "bool", "b", true, 8),
		*gengo.NewField("Testing", "float32", "f32", true, 2),
		*gengo.NewField("Testing", "float64", "f64", true, 1),
		*gengo.NewField("Testing", "string", "s", true, 3),
		*gengo.NewField("Testing", "time", "t", true, 2),
		*gengo.NewField("Testing", "duration", "d", true, 2),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

var fixedArraySerialized []byte = []byte{
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u8
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u16
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u32
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u64
	0xfe, 0xff, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, // i8
	0xfe, 0xff, 0xff, 0xff, 0x00, 0x00, 0x01, 0x00, // i16
	0xfe, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, // i32
	0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // i64
	0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, // b
	0x2b, 0x52, 0x9a, 0x44, 0x2b, 0x52, 0x9a, 0x44, // f32
	0x98, 0xdd, 0x93, 0x87, 0x45, 0x4a, 0xc3, 0xc0, // f64
	0x05, 0x00, 0x00, 0x00, 'R', 'o', 'c', 'o', 's', // s[0]
	0x05, 0x00, 0x00, 0x00, 's', 'o', 'r', 'o', 'c', // s[1]
	0x05, 0x00, 0x00, 0x00, 'c', 'r', 'o', 'o', 's', // s[2]
	0x0d, 0xf0, 0xed, 0xfe, 0xef, 0xbe, 0x37, 0x13, // t[0]
	0xef, 0xbe, 0x37, 0x13, 0x0d, 0xf0, 0x37, 0x13, // t[1]
	0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x00, // d[0]
	0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, // d[1]
}

// Benchmark deserializing across all primitives as fixed array entries to the message type.
func BenchmarkDynamicMessage_Deserialize_FixedArrayPrimitives(b *testing.B) {

	testMessage := fixedArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(fixedArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Dynamic array type used for testing across all ROS primitives. Note: negative array sizes => dynamic arrays.
var dynamicArrayMessageType DynamicMessageType = DynamicMessageType{
	spec: generateTestSpec([]gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", true, -1),
		*gengo.NewField("Testing", "uint16", "u16", true, -1),
		*gengo.NewField("Testing", "uint32", "u32", true, -1),
		*gengo.NewField("Testing", "uint64", "u64", true, -1),
		*gengo.NewField("Testing", "int8", "i8", true, -1),
		*gengo.NewField("Testing", "int16", "i16", true, -1),
		*gengo.NewField("Testing", "int32", "i32", true, -1),
		*gengo.NewField("Testing", "int64", "i64", true, -1),
		*gengo.NewField("Testing", "bool", "b", true, -1),
		*gengo.NewField("Testing", "float32", "f32", true, -1),
		*gengo.NewField("Testing", "float64", "f64", true, -1),
		*gengo.NewField("Testing", "string", "s", true, -1),
		*gengo.NewField("Testing", "time", "t", true, -1),
		*gengo.NewField("Testing", "duration", "d", true, -1),
	}),
	nested:       make(map[string]*DynamicMessageType),
	jsonPrealloc: 0,
}

var dynamicArraySerialized []byte = []byte{
	0x08, 0x00, 0x00, 0x00, // Dynamic array size.
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u8
	0x04, 0x00, 0x00, 0x00, // Dynamic array size.
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u16
	0x02, 0x00, 0x00, 0x00, // Dynamic array size.
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u32
	0x01, 0x00, 0x00, 0x00, // Dynamic array size.
	0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, // u64
	0x08, 0x00, 0x00, 0x00, // Dynamic array size.
	0xfe, 0xff, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, // i8
	0x04, 0x00, 0x00, 0x00, // Dynamic array size.
	0xfe, 0xff, 0xff, 0xff, 0x00, 0x00, 0x01, 0x00, // i16
	0x02, 0x00, 0x00, 0x00, // Dynamic array size.
	0xfe, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, // i32
	0x01, 0x00, 0x00, 0x00, // Dynamic array size.
	0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // i64
	0x08, 0x00, 0x00, 0x00, // Dynamic array size.
	0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, // b
	0x02, 0x00, 0x00, 0x00, // Dynamic array size.
	0x2b, 0x52, 0x9a, 0x44, 0x2b, 0x52, 0x9a, 0x44, // f32
	0x01, 0x00, 0x00, 0x00, // Dynamic array size.
	0x98, 0xdd, 0x93, 0x87, 0x45, 0x4a, 0xc3, 0xc0, // f64
	0x03, 0x00, 0x00, 0x00, // Dynamic array size.
	0x05, 0x00, 0x00, 0x00, 'R', 'o', 'c', 'o', 's', // s[0]
	0x05, 0x00, 0x00, 0x00, 's', 'o', 'r', 'o', 'c', // s[1]
	0x05, 0x00, 0x00, 0x00, 'c', 'r', 'o', 'o', 's', // s[2]
	0x02, 0x00, 0x00, 0x00, // Dynamic array size.
	0x0d, 0xf0, 0xed, 0xfe, 0xef, 0xbe, 0x37, 0x13, // t[0]
	0xef, 0xbe, 0x37, 0x13, 0x0d, 0xf0, 0x37, 0x13, // t[1]
	0x02, 0x00, 0x00, 0x00, // Dynamic array size.
	0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x00, // d[0]
	0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, // d[1]
}

// Benchmark deserializing across all primitives as dynamic array entries to the message type.
func BenchmarkDynamicMessage_Deserialize_DynamicArrayMedley(b *testing.B) {

	testMessage := dynamicArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(dynamicArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Big array benchmarks.

// One megabyte array of zeros, used for most big array benchmarks.
var bigArraySerialized []byte = make([]byte, 1_000_000)

// Benchmark deserializing a one megabyte array of booleans.
func BenchmarkDynamicMessage_Deserialize_boolBigArray(b *testing.B) {

	var boolBigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "bool", "b", true, 1_000_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := boolBigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of int8.
func BenchmarkDynamicMessage_Deserialize_int8BigArray(b *testing.B) {

	var int8BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "int8", "i8", true, 1_000_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := int8BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of int16.
func BenchmarkDynamicMessage_Deserialize_int16BigArray(b *testing.B) {

	var int16BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "int16", "i16", true, 500_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := int16BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of int32.
func BenchmarkDynamicMessage_Deserialize_int32BigArray(b *testing.B) {
	var int32BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "int32", "i32", true, 250_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := int32BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of int64.
func BenchmarkDynamicMessage_Deserialize_int64BigArray(b *testing.B) {

	var int64BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "int64", "i64", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := int64BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of uint8.
func BenchmarkDynamicMessage_Deserialize_uint8BigArray(b *testing.B) {

	var uint8BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint8", "u8", true, 1_000_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := uint8BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of uint16.
func BenchmarkDynamicMessage_Deserialize_uint16BigArray(b *testing.B) {

	var uint16BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint16", "u16", true, 500_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := uint16BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of uint32.
func BenchmarkDynamicMessage_Deserialize_uint32BigArray(b *testing.B) {

	var uint32BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint32", "u32", true, 250_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := uint32BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of uint64.
func BenchmarkDynamicMessage_Deserialize_uint64BigArray(b *testing.B) {

	var uint64BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint64", "u64", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := uint64BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of float32.
func BenchmarkDynamicMessage_Deserialize_float32BigArray(b *testing.B) {

	var float32BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "float32", "f32", true, 250_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := float32BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of float64.
func BenchmarkDynamicMessage_Deserialize_float64BigArray(b *testing.B) {

	var float64BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "float64", "f64", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := float64BigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of "JoJo" strings.
func BenchmarkDynamicMessage_Deserialize_stringBigArray(b *testing.B) {

	var stringBigArraySerialized []byte = bytes.Repeat([]byte{0x04, 0x00, 0x00, 0x00, 'J', 'o', 'J', 'o'}, 125_000)

	var stringBigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "string", "s", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := stringBigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(stringBigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of Time structs.
func BenchmarkDynamicMessage_Deserialize_timeBigArray(b *testing.B) {

	var timeBigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "time", "t", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := timeBigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmark deserializing a one megabyte array of Duration structs.
func BenchmarkDynamicMessage_Deserialize_durationBigArray(b *testing.B) {

	var durationBigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "duration", "d", true, 125_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	testMessage := durationBigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmarks for deserializing types embedded in types

// Benchmark deserializing a message with an embedded message.
func BenchmarkDynamicMessage_Deserialize_dynamicType(b *testing.B) {
	poseMessageType, err := NewDynamicMessageType("geometry_msgs/Pose")

	if err != nil {
		b.Skip("benchmark skipped, ROS environment not set up")
		return
	}

	if len(poseMessageType.spec.Fields) != 2 {
		b.Fatalf("expected 2 pose fields")
	}

	// Pose has 7 float64 values, 7 x 8 bytes = 56 bytes.
	slice := make([]byte, 56)

	testMessage := poseMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(slice)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("Deserialize pose failed!")
		}
	}
}

// Benchmark deserializing a one megabyte array of a message with an embedded message.
func BenchmarkDynamicMessage_Deserialize_dynamicTypeBigArray(b *testing.B) {
	// Extract 1_000_000 bytes / 56 (bytes/packet) ~= 17857 packet.
	var dynamicTypeBigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("geometry_msgs", "Pose", "pose", true, 17857),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	msgType, err := newDynamicMessageTypeNested("Pose", "geometry_msgs", nil, nil)
	if err != nil {
		b.Skip("benchmark skipped, ROS environment not set up")
		return
	}
	dynamicTypeBigArrayMessageType.nested["geometry_msgs/Pose"] = msgType

	testMessage := dynamicTypeBigArrayMessageType.NewDynamicMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteReader := bytes.NewReader(bigArraySerialized)
		if err := testMessage.Deserialize(byteReader); err != nil {
			b.Fatalf("deserialize failed %s", err)
		}
	}
}

// Benchmarks on NewDynamicMessage construction.

func BenchmarkDynamicMessage_NewMessage_Singular(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testMessage := singularMessageType.NewDynamicMessage()
		_ = testMessage.data
	}
}

func BenchmarkDynamicMessage_NewMessage_FixedArray(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testMessage := fixedArrayMessageType.NewDynamicMessage()
		_ = testMessage.data
	}
}

func BenchmarkDynamicMessage_NewMessage_DynamicArray(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testMessage := dynamicArrayMessageType.NewDynamicMessage()
		_ = testMessage.data
	}
}

func BenchmarkDynamicMessage_NewMessage_dynamicType(b *testing.B) {
	poseMessageType, err := NewDynamicMessageType("geometry_msgs/Pose")

	if err != nil {
		b.Skipf("benchmark skipped, ROS environment not set up")
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testMessage := poseMessageType.NewDynamicMessage()
		_ = testMessage.data
	}
}

func BenchmarkDynamicMessage_NewMessage_BigArray(b *testing.B) {
	var uint16BigArrayMessageType DynamicMessageType = DynamicMessageType{
		spec: generateTestSpec([]gengo.Field{
			*gengo.NewField("Testing", "uint16", "u16", true, 500_000),
		}),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testMessage := uint16BigArrayMessageType.NewDynamicMessage()
		_ = testMessage.data
	}
}
