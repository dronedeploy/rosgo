package ros

import (
	"bytes"
	goContext "context"
	"net"
)

type TCPRosReadResult struct {
	Buf []byte
	Err error
}

var decoder ByteDecoder = &LEByteDecoder{}

func readTCPRosSize(ctx goContext.Context, conn net.Conn) (uint32, error) {
	buf := make([]byte, 4) // TODO: leverage off readTCPRosData to remove duplication
	index := 0
	for {
		select {
		case <-ctx.Done():
			return 0, nil
		default:
			n, err := conn.Read(buf[index:])
			if err != nil {
				return 0, err
			}
			index += n
			if index >= len(buf) {
				return decoder.DecodeUint32(bytes.NewReader(buf))
			}
		}
	}
}

func readTCPRosData(ctx goContext.Context, conn net.Conn, size uint32) ([]byte, error) {
	buf := make([]byte, int(size))
	index := 0
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			n, err := conn.Read(buf[index:])
			if err != nil {
				return nil, err
			}
			index += n
			if index >= len(buf) {
				return buf, nil
			}
		}
	}
}

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

	// TODO: Error if size is too large
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
