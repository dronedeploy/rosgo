package ros

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

// LEByteDecoder is a little-endian byte decoder, implements the ByteDecoder interface.
type LEByteDecoder struct{}

var _ ByteDecoder = LEByteDecoder{}

// Helpers

func CheckSize(buf *bytes.Reader, size int) error {
	if size < 0 {
		return errors.New("unexpected negative size")
	}
	if buf.Len() < size {
		return errors.New("buffer size too small")
	}
	return nil
}

// Array decoders.

// DecodeBoolArray decodes an array of boolean values.
func (d LEByteDecoder) DecodeBoolArray(buf *bytes.Reader, size int) ([]bool, error) {
	if err := CheckSize(buf, size); err != nil {
		return nil, errors.Wrap(err, "decoding bool array")
	}

	var arr [1]byte
	slice := make([]bool, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 1 || err != nil {
			return slice, errors.New("Could not read 1 byte from buffer")
		}
		slice[i] = (arr[0] != 0x00)
	}
	return slice, nil
}

// DecodeInt8Array decodes an array of int8 values.
func (d LEByteDecoder) DecodeInt8Array(buf *bytes.Reader, size int) ([]int8, error) {
	if err := CheckSize(buf, size); err != nil {
		return nil, errors.Wrap(err, "decoding i8 array")
	}

	var arr [1]byte
	slice := make([]int8, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 1 || err != nil {
			return slice, errors.New("Could not read 1 byte from buffer")
		}
		slice[i] = int8(arr[0])
	}

	return slice, nil
}

// DecodeUint8Array decodes an array of uint8 values.
func (d LEByteDecoder) DecodeUint8Array(buf *bytes.Reader, size int) ([]uint8, error) {
	if err := CheckSize(buf, size); err != nil {
		return nil, errors.Wrap(err, "decoding u8 array")
	}

	slice := make([]uint8, size)
	if size == 0 {
		// Early return to avoid potentail EOF error.
		return slice, nil
	}
	n, err := buf.Read(slice)
	if n != size || err != nil {
		return slice, errors.New("Did not read entire uint8 buffer")
	}

	return slice, nil
}

// DecodeInt16Array decodes an array of int16 values.
func (d LEByteDecoder) DecodeInt16Array(buf *bytes.Reader, size int) ([]int16, error) {
	if err := CheckSize(buf, size*2); err != nil {
		return nil, errors.Wrap(err, "decoding i16 array")
	}

	var arr [2]byte
	slice := make([]int16, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 2 || err != nil {
			return slice, errors.New("Could not read 2 bytes from buffer")
		}
		slice[i] = int16(binary.LittleEndian.Uint16(arr[:]))
	}

	return slice, nil
}

// DecodeUint16Array decodes an array of uint16 values.
func (d LEByteDecoder) DecodeUint16Array(buf *bytes.Reader, size int) ([]uint16, error) {
	if err := CheckSize(buf, size*2); err != nil {
		return nil, errors.Wrap(err, "decoding u16 array")
	}

	var arr [2]byte
	slice := make([]uint16, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 2 || err != nil {
			return slice, errors.New("Could not read 2 bytes from buffer")
		}
		slice[i] = binary.LittleEndian.Uint16(arr[:])
	}

	return slice, nil
}

// DecodeInt32Array decodes an array of int32 values.
func (d LEByteDecoder) DecodeInt32Array(buf *bytes.Reader, size int) ([]int32, error) {
	if err := CheckSize(buf, size*4); err != nil {
		return nil, errors.Wrap(err, "decoding i32 array")
	}

	var arr [4]byte
	slice := make([]int32, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 4 || err != nil {
			return slice, errors.New("Could not read 4 bytes from buffer")
		}
		slice[i] = int32(binary.LittleEndian.Uint32(arr[:]))
	}

	return slice, nil
}

// DecodeUint32Array decodes an array of uint32 values.
func (d LEByteDecoder) DecodeUint32Array(buf *bytes.Reader, size int) ([]uint32, error) {
	if err := CheckSize(buf, size*4); err != nil {
		return nil, errors.Wrap(err, "decoding u32 array")
	}

	var arr [4]byte
	slice := make([]uint32, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 4 || err != nil {
			return slice, errors.New("Could not read 4 bytes from buffer")
		}
		slice[i] = binary.LittleEndian.Uint32(arr[:])
	}

	return slice, nil
}

// DecodeFloat32Array decodes an array of float32 values.
func (d LEByteDecoder) DecodeFloat32Array(buf *bytes.Reader, size int) ([]JsonFloat32, error) {
	if err := CheckSize(buf, size*4); err != nil {
		return nil, errors.Wrap(err, "decoding f32 array")
	}

	var arr [4]byte
	var value float32
	slice := make([]JsonFloat32, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 4 || err != nil {
			return slice, errors.New("Could not read 4 bytes from buffer")
		}
		value = math.Float32frombits(binary.LittleEndian.Uint32(arr[:]))
		slice[i] = JsonFloat32{F: value}
	}

	return slice, nil
}

// DecodeInt64Array decodes an array of int64 values.
func (d LEByteDecoder) DecodeInt64Array(buf *bytes.Reader, size int) ([]int64, error) {
	if err := CheckSize(buf, size*8); err != nil {
		return nil, errors.Wrap(err, "decoding i64 array")
	}

	var arr [8]byte
	slice := make([]int64, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 8 || err != nil {
			return slice, errors.New("Could not read 8 bytes from buffer")
		}
		slice[i] = int64(binary.LittleEndian.Uint64(arr[:]))
	}

	return slice, nil
}

// DecodeUint64Array decodes an array of uint64 values.
func (d LEByteDecoder) DecodeUint64Array(buf *bytes.Reader, size int) ([]uint64, error) {
	if err := CheckSize(buf, size*8); err != nil {
		return nil, errors.Wrap(err, "decoding u64 array")
	}

	var arr [8]byte
	slice := make([]uint64, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 8 || err != nil {
			return slice, errors.New("Could not read 8 bytes from buffer")
		}
		slice[i] = binary.LittleEndian.Uint64(arr[:])
	}

	return slice, nil
}

// DecodeFloat64Array decodes an array of float64 values.
func (d LEByteDecoder) DecodeFloat64Array(buf *bytes.Reader, size int) ([]JsonFloat64, error) {
	if err := CheckSize(buf, size*8); err != nil {
		return nil, errors.Wrap(err, "decoding f64 array")
	}

	var arr [8]byte
	var value float64
	slice := make([]JsonFloat64, size)
	for i := 0; i < size; i++ {
		if n, err := buf.Read(arr[:]); n != 8 || err != nil {
			return slice, errors.New("Could not read 8 bytes from buffer")
		}
		value = math.Float64frombits(binary.LittleEndian.Uint64(arr[:]))
		slice[i] = JsonFloat64{F: value}
	}

	return slice, nil
}

// DecodeStringArray decodes an array of strings.
func (d LEByteDecoder) DecodeStringArray(buf *bytes.Reader, size int) ([]string, error) {
	// Use minimum string byte size = 4
	if err := CheckSize(buf, size*4); err != nil {
		return nil, errors.Wrap(err, "decoding string array")
	}

	// String format is: [size|string] where size is a u32.
	slice := make([]string, size)
	for i := 0; i < size; i++ {
		var strSize uint32
		var err error
		if strSize, err = d.DecodeUint32(buf); err != nil {
			return slice, errors.Wrap(err, "decoding string array")
		}

		var value []uint8
		value, err = d.DecodeUint8Array(buf, int(strSize))
		if err != nil {
			return slice, errors.Wrap(err, "decoding string array")
		}
		slice[i] = string(value)
	}
	return slice, nil
}

// DecodeTimeArray decodes an array of Time structs.
func (d LEByteDecoder) DecodeTimeArray(buf *bytes.Reader, size int) ([]Time, error) {
	if err := CheckSize(buf, size*8); err != nil {
		return nil, errors.Wrap(err, "decoding time array")
	}

	// Time format is: [sec|nanosec] where sec and nanosec are unsigned integers.
	slice := make([]Time, size)
	for i := 0; i < size; i++ {
		var err error
		if slice[i].Sec, err = d.DecodeUint32(buf); err != nil {
			return slice, errors.Wrap(err, "decoding time array")
		}
		if slice[i].NSec, err = d.DecodeUint32(buf); err != nil {
			return slice, errors.Wrap(err, "decoding time array")
		}
	}
	return slice, nil
}

// DecodeDurationArray decodes an array of Duration structs.
func (d LEByteDecoder) DecodeDurationArray(buf *bytes.Reader, size int) ([]Duration, error) {
	if err := CheckSize(buf, size*8); err != nil {
		return nil, errors.Wrap(err, "decoding duration array")
	}

	// Duration format is: [sec|nanosec] where sec and nanosec are unsigned integers.
	slice := make([]Duration, size)
	for i := 0; i < size; i++ {
		var err error
		if slice[i].Sec, err = d.DecodeUint32(buf); err != nil {
			return slice, errors.Wrap(err, "decoding duration array")
		}
		if slice[i].NSec, err = d.DecodeUint32(buf); err != nil {
			return slice, errors.Wrap(err, "decoding duration array")
		}
	}
	return slice, nil
}

// DecodeMessageArray decodes an array of DynamicMessages.
func (d LEByteDecoder) DecodeMessageArray(buf *bytes.Reader, size int, msgType *DynamicMessageType) ([]Message, error) {
	// Not an exact check, but at least prevents an impossible allocation
	if err := CheckSize(buf, size); err != nil {
		return nil, errors.Wrap(err, "decoding message array")
	}

	slice := make([]Message, size)

	for i := 0; i < size; i++ {
		// Skip the zero value initialization, this would just get discarded anyway.
		msg := &DynamicMessage{}
		msg.dynamicType = msgType
		if err := msg.Deserialize(buf); err != nil {
			return slice, errors.Wrap(err, "decoding message array")
		}
		slice[i] = msg
	}
	return slice, nil
}

// Singular decodes.

// DecodeBool decodes a boolean.
func (d LEByteDecoder) DecodeBool(buf *bytes.Reader) (bool, error) {
	raw, err := d.DecodeUint8(buf)
	return (raw != 0x00), err
}

// DecodeInt8 decodes a int8.
func (d LEByteDecoder) DecodeInt8(buf *bytes.Reader) (int8, error) {
	raw, err := d.DecodeUint8(buf)
	return int8(raw), err
}

// DecodeUint8 decodes a uint8.
func (d LEByteDecoder) DecodeUint8(buf *bytes.Reader) (uint8, error) {
	var arr [1]byte

	if n, err := buf.Read(arr[:]); n != 1 || err != nil {
		return 0, errors.New("Could not read 1 byte from buffer")
	}

	return arr[0], nil
}

// DecodeInt16 decodes a int16.
func (d LEByteDecoder) DecodeInt16(buf *bytes.Reader) (int16, error) {
	raw, err := d.DecodeUint16(buf)
	return int16(raw), err
}

// DecodeUint16 decodes a uint16.
func (d LEByteDecoder) DecodeUint16(buf *bytes.Reader) (uint16, error) {
	var arr [2]byte

	if n, err := buf.Read(arr[:]); n != 2 || err != nil {
		return 0, errors.New("Could not read 2 bytes from buffer")
	}

	return binary.LittleEndian.Uint16(arr[:]), nil
}

// DecodeInt32 decodes a int32.
func (d LEByteDecoder) DecodeInt32(buf *bytes.Reader) (int32, error) {
	raw, err := d.DecodeUint32(buf)
	return int32(raw), err
}

// DecodeUint32 decodes a uint32.
func (d LEByteDecoder) DecodeUint32(buf *bytes.Reader) (uint32, error) {
	var arr [4]byte

	if n, err := buf.Read(arr[:]); n != 4 || err != nil {
		return 0, errors.New("Could not read 4 bytes from buffer")
	}

	return binary.LittleEndian.Uint32(arr[:]), nil
}

// DecodeFloat32 decodes a JsonFloat32.
func (d LEByteDecoder) DecodeFloat32(buf *bytes.Reader) (JsonFloat32, error) {
	raw, err := d.DecodeUint32(buf)
	return JsonFloat32{F: math.Float32frombits(raw)}, err
}

// DecodeInt64 decodes a int64.
func (d LEByteDecoder) DecodeInt64(buf *bytes.Reader) (int64, error) {
	raw, err := d.DecodeUint64(buf)
	return int64(raw), err
}

// DecodeUint64 decodes a uint64.
func (d LEByteDecoder) DecodeUint64(buf *bytes.Reader) (uint64, error) {
	var arr [8]byte

	if n, err := buf.Read(arr[:]); n != 8 || err != nil {
		return 0, errors.New("Could not read 8 bytes from buffer")
	}

	return binary.LittleEndian.Uint64(arr[:]), nil
}

// DecodeFloat64 decodes a JsonFloat64.
func (d LEByteDecoder) DecodeFloat64(buf *bytes.Reader) (JsonFloat64, error) {
	raw, err := d.DecodeUint64(buf)
	return JsonFloat64{F: math.Float64frombits(raw)}, err
}

// DecodeString decodes a string.
func (d LEByteDecoder) DecodeString(buf *bytes.Reader) (string, error) {
	var err error
	var strSize uint32
	// String format is: [size|string] where size is a u32.
	if strSize, err = d.DecodeUint32(buf); err != nil {
		return "", errors.Wrap(err, "decoding string")
	}
	var value []uint8
	if value, err = d.DecodeUint8Array(buf, int(strSize)); err != nil {
		return "", errors.Wrap(err, "decoding string")
	}
	return string(value), nil
}

// DecodeTime decodes a Time struct.
func (d LEByteDecoder) DecodeTime(buf *bytes.Reader) (Time, error) {
	var err error
	var value Time

	// Time format is: [sec|nanosec] where sec and nanosec are unsigned integers.
	if value.Sec, err = d.DecodeUint32(buf); err != nil {
		return Time{}, errors.Wrap(err, "decoding time")
	}
	if value.NSec, err = d.DecodeUint32(buf); err != nil {
		return Time{}, errors.Wrap(err, "decoding time")
	}

	return value, nil
}

// DecodeDuration decodes a Duraction struct.
func (d LEByteDecoder) DecodeDuration(buf *bytes.Reader) (Duration, error) {
	var err error
	var value Duration

	// Duration format is: [sec|nanosec] where sec and nanosec are unsigned integers.
	if value.Sec, err = d.DecodeUint32(buf); err != nil {
		return Duration{}, errors.Wrap(err, "decoding duration")
	}
	if value.NSec, err = d.DecodeUint32(buf); err != nil {
		return Duration{}, errors.Wrap(err, "decoding duration")
	}

	return value, nil
}

// DecodeMessage decodes a DynamicMessage.
func (d LEByteDecoder) DecodeMessage(buf *bytes.Reader, msgType *DynamicMessageType) (Message, error) {
	// Skip the zero value initialization, this would just get discarded anyway.
	msg := &DynamicMessage{}
	msg.dynamicType = msgType
	if err := msg.Deserialize(buf); err != nil {
		return nil, errors.Wrap(err, "decoding message")
	}

	return msg, nil
}
