package ros

import (
	"bytes"
	"testing"

	gengo "github.com/dronedeploy/rosgo/libgengo"
)

func TestCheckSize(t *testing.T) {
	raw := make([]byte, 10)
	buffer := bytes.NewReader(raw)

	if err := CheckSize(buffer, -1); err == nil {
		t.Fatalf("negative size should result in error")
	}

	if err := CheckSize(buffer, 9); err != nil {
		t.Fatalf("expected pass")
	}

	if err := CheckSize(buffer, 10); err != nil {
		t.Fatalf("exact match should pass")
	}

	if err := CheckSize(buffer, 11); err == nil {
		t.Fatalf("size greater than buffer should fail")
	}

	read := make([]byte, 10)
	buffer.Read(read)

	if err := CheckSize(buffer, -1); err == nil {
		t.Fatalf("negative size should result in error")
	}

	if err := CheckSize(buffer, 0); err != nil {
		t.Fatalf("exact match should pass")
	}

	if err := CheckSize(buffer, 1); err == nil {
		t.Fatalf("size greater than buffer should fail")
	}
}

func TestDecoder_BufferTooSmall(t *testing.T) {
	raw := make([]byte, 0)
	buffer := bytes.NewReader(raw)
	d := LEByteDecoder{}

	fields := []gengo.Field{
		*gengo.NewField("Testing", "uint8", "v", false, 0),
	}
	testMessageType := &DynamicMessageType{
		spec:         generateTestSpec(fields),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	if _, err := d.DecodeBoolArray(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt8Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt16Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt32Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt64Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint8Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint16Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint32Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint64Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat32Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat64Array(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeStringArray(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeTimeArray(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeDurationArray(buffer, 1); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeMessageArray(buffer, 1, testMessageType); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeBool(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt8(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt16(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt32(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt64(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint8(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint16(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint32(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint64(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat32(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat64(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeString(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeTime(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeDuration(buffer); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeMessage(buffer, testMessageType); err == nil {
		t.Fatal("did not error for buffer too small")
	}

}

func TestDecoder_DangerousSize(t *testing.T) {
	raw := make([]byte, 20)
	buffer := bytes.NewReader(raw)
	d := LEByteDecoder{}

	fields := []gengo.Field{
		*gengo.NewField("Testing", "uint8", "v", false, 0),
	}
	testMessageType := &DynamicMessageType{
		spec:         generateTestSpec(fields),
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}

	// These test cases are designed to cause the tester to throw if we preallocate without checking size.

	if _, err := d.DecodeBoolArray(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt8Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt16Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt32Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeInt64Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint8Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint16Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint32Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeUint64Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat32Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeFloat64Array(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeStringArray(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeTimeArray(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeDurationArray(buffer, 1e9); err == nil {
		t.Fatal("did not error for buffer too small")
	}
	if _, err := d.DecodeMessageArray(buffer, 1e9, testMessageType); err == nil {
		t.Fatal("did not error for buffer too small")
	}
}

func TestDecoder_BufferExactSize(t *testing.T) {
	raw := []byte{0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	buffer := bytes.NewReader(raw)
	d := LEByteDecoder{}

	if _, err := d.DecodeBoolArray(buffer, 16); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeInt8Array(buffer, 16); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeInt16Array(buffer, 8); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeInt32Array(buffer, 4); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeInt64Array(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeUint8Array(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeUint16Array(buffer, 8); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeUint32Array(buffer, 4); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeUint64Array(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeFloat32Array(buffer, 4); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeFloat64Array(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeStringArray(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buffer = bytes.NewReader(raw)
	if _, err := d.DecodeTimeArray(buffer, 2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
