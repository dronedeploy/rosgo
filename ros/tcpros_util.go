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
const tcpRosWriteTimeout time.Duration = 100 * time.Millisecond

// TCPRosDialer interface used for connecting to servers.
type TCPRosDialer interface {
	Dial(ctx goContext.Context, uri string) (net.Conn, error)
}

// TCPRosNetDialer implements the TCPRosDialer for net connections.
type TCPRosNetDialer struct{}

// Dial will attempt to connect to the specified uri over tcp.
func (d *TCPRosNetDialer) Dial(ctx goContext.Context, uri string) (net.Conn, error) {
	nd := net.Dialer{}
	return nd.DialContext(ctx, "tcp", uri)
}

var _ TCPRosDialer = &TCPRosNetDialer{}

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

// readTcpRosMessage performs asynchronous TCPROS message reads.
// Once it is done, it sends a result on the result channel, unless the context has been cancelled.
func readTCPRosMessage(ctx goContext.Context, conn net.Conn, resultChan chan TCPRosReadResult) {
	size, err := readTCPRosSize(ctx, conn)

	select { // Bail if the context has been cancelled.
	case <-ctx.Done():
		return
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
	select { // Bail if the context has been cancelled.
	case <-ctx.Done():
		return
	default:
	}

	resultChan <- TCPRosReadResult{data, err}
}

// writeTcpRosMessage performs asynchronous TCPROS message writes.
// Once it is done, it sends a result on the result channel, unless the context has been cancelled.
func writeTCPRosMessage(ctx goContext.Context, conn net.Conn, msgBuf []byte, resultChan chan error) {

	err := writeTCPRosSize(ctx, conn, uint32(len(msgBuf)))

	select { // Bail if the context has been cancelled.
	case <-ctx.Done():
		return
	default:
	}

	if err != nil {
		resultChan <- err
		return
	}

	err = writeTCPRosBuf(ctx, conn, msgBuf)

	select { // Bail if the context has been cancelled.
	case <-ctx.Done():
		return
	default:
	}

	resultChan <- err
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

	for index < 4 {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn.SetWriteDeadline(time.Now().Add(tcpRosWriteTimeout))
			n, err := conn.Write(buff.Bytes()[index:])
			if err != nil {
				return err
			}
			index += n
		}
	}
	return nil
}

func writeTCPRosBuf(ctx goContext.Context, conn net.Conn, buf []byte) error {
	for len(buf) > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn.SetWriteDeadline(time.Now().Add(tcpRosWriteTimeout))
			n, err := conn.Write(buf)
			if err != nil {
				return err
			}
			buf = buf[n:]
		}
	}
	return nil
}
