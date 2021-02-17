package ros

import (
	"bytes"
	goContext "context"
	"encoding/binary"
	"net"
	"time"
)

// Technically, this value isn't specified in the ROS framework. However, packets above 10 MB take a lot of effort to handle.
const maximumTCPRosMessageSize uint32 = 250_000_000

const tcpRosReadTimeout time.Duration = 100 * time.Millisecond

// TCPRosError defines custom error types returned by readTCPRosMessage and writeTCPRosMessage. Not all errors returned will be TCPRosErrors.
type TCPRosError int

// All TCPRosError types.
const (
	TCPRosErrorSizeTooLarge TCPRosError = iota
)

func (err *TCPRosError) Error() string {
	switch *err {
	case TCPRosErrorSizeTooLarge:
		return "message size is too large"
	default:
		return "unknown TCPRosError"
	}
}

// TCPRosReadResult defines a structure which is delivered via a channel when calling the asynchronous readTCPRosMessage.
type TCPRosReadResult struct {
	Buf []byte
	Err error
}

var decoder ByteDecoder = &LEByteDecoder{}

// readTcpRosMessage is a rosgo library utility for performing common reads of ros messages from a connection.
// it should always be called as a go routine, and will return an error on the result channel when it returns.
func readTCPRosMessage(ctx goContext.Context, conn net.Conn, resultChan chan TCPRosReadResult) {

	size, err := readTCPRosSize(ctx, conn)

	select {
	case <-ctx.Done():
		return // Bail if the context has been cancelled.
	default:
	}

	if err != nil {
		resultChan <- TCPRosReadResult{nil, err}
		return
	}

	if size >= maximumTCPRosMessageSize {
		tcpRosErr := TCPRosError(TCPRosErrorSizeTooLarge)
		resultChan <- TCPRosReadResult{nil, &tcpRosErr}
		return
	}

	data, err := readTCPRosData(ctx, conn, size)
	select {
	case <-ctx.Done():
		return // Bail if the context has been cancelled.
	default:
	}

	if err != nil {
		resultChan <- TCPRosReadResult{nil, err}
		return
	}

	resultChan <- TCPRosReadResult{data, nil}
}

func writeTCPRosMessage(ctx goContext.Context, conn net.Conn, msgBuf []byte, resultChan chan error) {

	err := writeTCPRosSize(ctx, conn, uint32(len(msgBuf)))

	select {
	case <-ctx.Done():
		return
	default:
	}

	if err != nil {
		resultChan <- err
	}

	_, err = conn.Write(msgBuf)
	if err != nil {
		return
	}
	resultChan <- nil
}

// Local helper functions

func isTimeoutError(err error) bool {
	neterr, ok := err.(net.Error)
	return (ok && neterr.Timeout())
}

func readTCPRosSize(ctx goContext.Context, conn net.Conn) (uint32, error) {
	buf := make([]byte, 4)
	index := 0
	for index < len(buf) {
		select {
		case <-ctx.Done():
			return 0, nil
		default:

			conn.SetReadDeadline(time.Now().Add(tcpRosReadTimeout))
			n, err := conn.Read(buf[index:])
			if err != nil {
				// Timeouts are expected between messages, so just keep going. However a timeout during (not at the start of) a size read is considered an error.
				if isTimeoutError(err) == false || index != 0 {
					return 0, err
				}
			}
			index += n
		}
	}
	return decoder.DecodeUint32(bytes.NewReader(buf))
}

func readTCPRosData(ctx goContext.Context, conn net.Conn, size uint32) ([]byte, error) {
	buf := make([]byte, int(size))
	index := 0
	for index < len(buf) {
		select {
		case <-ctx.Done():
			return nil, nil
		default:

			conn.SetReadDeadline(time.Now().Add(tcpRosReadTimeout))
			n, err := conn.Read(buf[index:])
			if err != nil {
				return nil, err
			}
			index += n
		}
	}
	return buf, nil
}

func writeTCPRosSize(ctx goContext.Context, conn net.Conn, size uint32) error {
	buff := bytes.NewBuffer(make([]byte, 0, 4))
	binary.Write(buff, binary.LittleEndian, size)
	index := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			n, err := conn.Write(buff.Bytes()[index:])
			if err != nil {
				return err
			}
			// TODO: Condition is still here...  need to extend time when err is not timeout
			index += n
			if index >= 4 {
				return nil
			}
		}
	}
}
