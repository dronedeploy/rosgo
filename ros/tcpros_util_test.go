package ros

import (
	"bytes"
	goContext "context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// Private test strucutures.

// fakeContext is a context.Context interface fake.
type fakeContext struct {
	done chan struct{}
}

func newFakeContext() *fakeContext {
	ctx := fakeContext{}
	ctx.done = make(chan struct{})
	return &ctx
}

// Helper for cleanup in a test.
func (ctx *fakeContext) cancel(t *testing.T, d time.Duration) {
	close(ctx.done)
}

// Helper for cleanup in a test.
func (ctx *fakeContext) cleanUp() {
	select {
	case <-ctx.done:
		// Already closed!
	default:
		close(ctx.done)
	}
}

// Implementing the Context interface
func (ctx *fakeContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (ctx *fakeContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *fakeContext) Err() error {
	return nil
}

func (ctx *fakeContext) Value(key interface{}) interface{} {
	return nil
}

var _ goContext.Context = &fakeContext{}

func intMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// Fake Connection
type fakeAddr struct{ string }

func (a *fakeAddr) Network() string {
	return "tcp"
}

func (a *fakeAddr) String() string {
	return a.string
}

var _ net.Addr = &fakeAddr{""}

type fakeConnReadResponse struct {
	Buf []byte
	Err error
}

type fakeConnWriteResponse struct {
	N   int
	Err error
}

type fakeConn struct {
	readRequest   chan int
	readResponse  chan fakeConnReadResponse
	writeRequest  chan []byte
	writeResponse chan fakeConnWriteResponse
	localAddr     fakeAddr
	remoteAddr    fakeAddr
	readDeadline  time.Time
	writeDeadline time.Time
}

func newFakeConn() *fakeConn {
	c := &fakeConn{}
	c.readRequest = make(chan int)
	c.readResponse = make(chan fakeConnReadResponse)
	c.writeRequest = make(chan []byte)
	c.writeResponse = make(chan fakeConnWriteResponse)
	c.localAddr = fakeAddr{"127.0.0.1:1"}
	c.remoteAddr = fakeAddr{"127.0.0.1:2"}
	return c
}

func (c *fakeConn) Read(b []byte) (n int, err error) {
	// Send request to test case, get response.
	c.readRequest <- len(b)
	response := <-c.readResponse

	if response.Err != nil {
		return 0, response.Err
	}

	n = copy(b, response.Buf)

	return n, nil
}

func (c *fakeConn) Write(b []byte) (int, error) {
	// Send request to test case, get response.
	c.writeRequest <- b
	response := <-c.writeResponse
	return response.N, response.Err
}

func (c *fakeConn) Close() error         { return nil }
func (c *fakeConn) LocalAddr() net.Addr  { return &c.localAddr }
func (c *fakeConn) RemoteAddr() net.Addr { return &c.remoteAddr }

func (c *fakeConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}

func (c *fakeConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

func (c *fakeConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

var _ net.Conn = &fakeConn{}
var _ io.Reader = &fakeConn{}

// Tests start here.

// Tolerance for checking timing on reads. The results of these checks are not deterministic because we don't mock time yet.
const fuzzyTimeTolerance time.Duration = 100 * time.Millisecond

func TestReadTCPRosMessage_successfulCases(t *testing.T) {

	testCases := []struct {
		bytes    []byte
		expected []byte
	}{
		{ // Has data case.
			[]byte{0x01, 0x00, 0x00, 0x00, 'a'},
			[]byte{'a'},
		},
		{ // More data case.
			[]byte{0x06, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e', 'f'},
			[]byte{'a', 'b', 'c', 'd', 'e', 'f'},
		},
		{ // Only read up to the length assigned.
			[]byte{0x04, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 0x00, 0x00},
			[]byte{'a', 'b', 'c', 'd'},
		},
	}

	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()

	for i, testCase := range testCases {
		conn := newFakeConn()

		go readTCPRosMessage(ctx, conn, resultChan)

		conn.readDeadline = time.Time{}
		fuzzyDeadline := time.Now().Add(tcpRosReadTimeout)

		select {
		case request := <-conn.readRequest:
			if request != 4 {
				t.Fatalf("[%d]: expected to request 4 length bytes, got %d", i, request)
			}

			// Verify the deadline was set appropriately.
			fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
			if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
				t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
			}
			conn.readDeadline = time.Time{}

			conn.readResponse <- fakeConnReadResponse{
				Buf: testCase.bytes[:4],
				Err: nil,
			}

		case <-time.After(time.Second):
			t.Fatalf("[%d]: expected read request", i)
		}

		fuzzyDeadline = time.Now().Add(tcpRosReadTimeout)

		select {
		case request := <-conn.readRequest:
			if request != len(testCase.expected) {
				t.Fatalf("[%d]: expected to request %d length bytes, got %d", i, len(testCase.expected), request)
			}

			// Verify the deadline was set appropriately.
			fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
			if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
				t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
			}

			conn.readResponse <- fakeConnReadResponse{
				Buf: testCase.bytes[4:],
				Err: nil,
			}

		case <-time.After(time.Second):
			t.Fatalf("[%d]: expected read request", i)
		}

		select {
		case result := <-resultChan:
			if result.Err != nil {
				t.Fatalf("[%d]: unexpected error %v", i, result.Err)
			}
			if result.Buf == nil {
				t.Fatalf("[%d]: result buffer is nil", i)
			} else if string(result.Buf) != string(testCase.expected) {
				t.Fatalf("[%d]: buffer mismatch. Result: %v, expected: %v", i, result.Buf, testCase.expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("[%d]: expected to receive result", i)
		}
	}
}

func TestReadTCPRosMessage_successfulCasesDripFed(t *testing.T) {

	testCases := []struct {
		bytes    []byte
		expected []byte
	}{
		{ // Has data case.
			[]byte{0x01, 0x00, 0x00, 0x00, 'a'},
			[]byte{'a'},
		},
		{ // More data case.
			[]byte{0x06, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e', 'f'},
			[]byte{'a', 'b', 'c', 'd', 'e', 'f'},
		},
		{ // Only read up to the length assigned.
			[]byte{0x04, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 0x00, 0x00},
			[]byte{'a', 'b', 'c', 'd'},
		},
		{ // Big array.
			append([]byte{0xe8, 0x03, 0x00, 0x00, 'a', 'b', 'c', 'd'}, make([]byte, 996)...),
			append([]byte{'a', 'b', 'c', 'd'}, make([]byte, 996)...),
		},
	}

	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()

	for i, testCase := range testCases {
		conn := newFakeConn()

		go readTCPRosMessage(ctx, conn, resultChan)

		conn.readDeadline = time.Time{}
		fuzzyDeadline := time.Now().Add(tcpRosReadTimeout)

		index := 0

		for index < 4 {
			select {
			case request := <-conn.readRequest:
				expectedN := 4 - index
				if request != expectedN {
					t.Fatalf("[%d]: expected to request %d length bytes, got %d", i, expectedN, request)
				}
				// Verify the deadline was set appropriately.
				fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
				if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
					t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
				}
				// Reset the deadlines for the next read.
				conn.readDeadline = time.Time{}

				conn.readResponse <- fakeConnReadResponse{
					Buf: testCase.bytes[index : index+1],
					Err: nil,
				}
				index++
				fuzzyDeadline = time.Now().Add(tcpRosReadTimeout)
			case <-time.After(time.Second):
				t.Fatalf("[%d]: expected read request", i)
			}
		}

		for index < len(testCase.expected)+4 {
			select {
			case request := <-conn.readRequest:
				expectedN := len(testCase.expected) - (index - 4)
				if request != expectedN {
					t.Fatalf("[%d]: expected to request %d length bytes, got %d", i, expectedN, request)
				}
				// Verify the deadline was set appropriately.
				fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
				if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
					t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
				}
				// Reset the deadlines for the next read.
				conn.readDeadline = time.Time{}

				conn.readResponse <- fakeConnReadResponse{
					Buf: testCase.bytes[index : index+1],
					Err: nil,
				}
				index++
				fuzzyDeadline = time.Now().Add(tcpRosReadTimeout)

			case <-time.After(time.Second):
				t.Fatalf("[%d]: expected read request", i)
			}
		}

		select {
		case result := <-resultChan:
			if result.Err != nil {
				t.Fatalf("[%d]: unexpected error %v", i, result.Err)
			}
			if result.Buf == nil {
				t.Fatalf("[%d]: result buffer is nil", i)
			} else if string(result.Buf) != string(testCase.expected) {
				t.Fatalf("[%d]: buffer mismatch. Result: %v, expected: %v", i, result.Buf, testCase.expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("[%d]: expected to receive result", i)
		}
	}
}

func TestReadTCPRosMessage_emptyMessage(t *testing.T) {

	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()

	conn := newFakeConn()

	go readTCPRosMessage(ctx, conn, resultChan)

	conn.readDeadline = time.Time{}
	fuzzyDeadline := time.Now().Add(tcpRosReadTimeout)

	select {
	case request := <-conn.readRequest:
		if request != 4 {
			t.Fatalf("expected to request 4 length bytes, got %d", request)
		}
		// Verify the deadline was set appropriately.
		fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
		if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
			t.Fatalf("read deadline was not updated correctly, delta: %v", fuzzyDelta)
		}

		conn.readResponse <- fakeConnReadResponse{
			Buf: []byte{0x00, 0x00, 0x00, 0x00},
			Err: nil,
		}
	case <-time.After(time.Second):
		t.Fatalf("expected read request")
	}

	select {
	case result := <-resultChan:
		if result.Err != nil {
			t.Fatalf("unexpected error %v", result.Err)
		}
		if result.Buf == nil {
			t.Fatalf("result buffer is nil")
		} else if len(result.Buf) != 0 {
			t.Fatalf("buffer mismatch. Result: %v, expected: []", result.Buf)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected to receive result")
	}
}

func TestReadTCPRosMessage_whenCancelled_isSilent(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()

	go readTCPRosMessage(ctx, conn, resultChan)

	ctx.cancel(t, time.Second)

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			conn.readResponse <- fakeConnReadResponse{Buf: []byte{}, Err: nil}
		case result := <-resultChan:
			t.Fatalf("expected cancelled read to close silently, got buf: %v err: %v", result.Buf, result.Err)
		case <-time.After(time.Second):
			done = true
		}
	}
}

func TestReadTCPRosMessage_whenEOFError_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()
	go readTCPRosMessage(ctx, conn, resultChan)

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			conn.readResponse <- fakeConnReadResponse{Buf: []byte{}, Err: io.EOF}
		case result := <-resultChan:
			if result.Err != io.EOF {
				t.Fatalf("unexpected error %v", result.Err)
			}
			done = true
		case <-time.After(time.Second):
			t.Fatalf("expected to receive conn error")
		}
	}
}

func TestReadTCPRosMessage_whenTimeoutErrorWaitingForSize_continuesTrying(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()

	go readTCPRosMessage(ctx, conn, resultChan)

	readCount := 0

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			conn.readResponse <- fakeConnReadResponse{Buf: []byte{}, Err: os.ErrDeadlineExceeded}
			readCount++
			if readCount > 10 { // arbitrary attempts to continue.
				done = true
			}
		case result := <-resultChan:
			t.Fatalf("unexpected result returned early: %v", result.Err)
		}
	}
}

func TestReadTCPRosMessage_whenTimeoutErrorDuringSize_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()

	go readTCPRosMessage(ctx, conn, resultChan)

	readCount := 0

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			if readCount == 0 {
				conn.readResponse <- fakeConnReadResponse{Buf: []byte{0, 0, 0}, Err: nil}
			} else {
				conn.readResponse <- fakeConnReadResponse{Buf: []byte{}, Err: os.ErrDeadlineExceeded}
			}
			readCount++
		case result := <-resultChan:
			if readCount == 0 {
				t.Fatalf("result returned early: %v", result.Err)
			}
			if result.Err != os.ErrDeadlineExceeded {
				t.Fatalf("unexpected error %v", result.Err)
			}
			done = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected to receive timeout error")
		}
	}
}

func TestReadTCPRosMessage_whenTimeoutErrorDuringData_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()
	go readTCPRosMessage(ctx, conn, resultChan)

	readCount := 0

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			if readCount == 0 {
				conn.readResponse <- fakeConnReadResponse{Buf: []byte{0x02, 0x00, 0x00, 0x00}, Err: nil}
			} else if readCount == 1 {
				conn.readResponse <- fakeConnReadResponse{Buf: []byte{0x01}, Err: nil}
			} else {
				conn.readResponse <- fakeConnReadResponse{Buf: []byte{}, Err: os.ErrDeadlineExceeded}
			}
			readCount++
		case result := <-resultChan:
			if readCount == 0 {
				t.Fatalf("result returned early: %v", result.Err)
			}
			if result.Err != os.ErrDeadlineExceeded {
				t.Fatalf("unexpected error %v", result.Err)
			}
			done = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected to receive timeout error")
		}
	}
}

func TestReadTCPRosMessage_whenDataSizeIsTooHigh_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	defer ctx.cleanUp()
	conn := newFakeConn()
	writer := bytes.NewBuffer(make([]byte, 0, 4))
	// Write the maximum size.
	size := uint32(maximumTCPRosMessageSize)
	if err := binary.Write(writer, binary.LittleEndian, &size); err != nil {
		t.Fatalf("could not write size %v", err)
	}

	go readTCPRosMessage(ctx, conn, resultChan)

	readCount := 0

	done := false
	for done == false {
		select {
		case <-conn.readRequest:
			if readCount == 0 {
				conn.readResponse <- fakeConnReadResponse{Buf: writer.Bytes(), Err: nil}
			} else {
				t.Fatalf("requested additional reads after exceeding size limits")
			}
			readCount++
		case result := <-resultChan:
			if err, ok := result.Err.(*TCPRosError); ok {
				if *err != TCPRosErrorSizeTooLarge {
					t.Fatalf("unexpected error %v", result.Err)
				}
			} else {
				t.Fatalf("unexpected error %v", result.Err)
			}
			done = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected to receive size error")
		}
	}
}

// Write Message Tests

// func Test_writeTCPRosMessage_successfulCases(t *testing.T) {
// 	testCases := []struct {
// 		message  []byte
// 		expected []byte
// 	}{
// 		{ // Zero data case.
// 			[]byte{},
// 			[]byte{0x00, 0x00, 0x00, 0x00},
// 		},
// 		{ // Has data case.
// 			[]byte{'a'},
// 			[]byte{0x01, 0x00, 0x00, 0x00, 'a'},
// 		},
// 		{ // More data case.
// 			[]byte{'a', 'b', 'c', 'd', 'e', 'f'},
// 			[]byte{0x06, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e', 'f'},
// 		},
// 	}

// 	resultChan := make(chan error)
// 	ctx := newFakeContext()
// 	defer ctx.cleanUp()

// 	for i, testCase := range testCases {
// 		conn := newFakeConn()
// 		go writeTCPRosMessage(ctx, conn, testCase.message, resultChan)

// 		iter := 0

// 		for {
// 			<-time.After(time.Millisecond)
// 			if len(conn.writeBytes) == len(testCase.expected) {
// 				break
// 			}
// 			if iter++; iter > 1000 {
// 				t.Fatalf("[%d]: failed to write the correct number of bytes", i)
// 			}
// 		}

// 		select {
// 		case err := <-resultChan:
// 			if err != nil {
// 				t.Fatalf("[%d]: unexpected error %v", i, err)
// 			}
// 			if string(conn.writeBytes) != string(testCase.expected) {
// 				t.Fatalf("[%d]: buffer mismatch. Result: %v, expected: %v", i, conn.writeBytes, testCase.expected)
// 			}
// 		case <-time.After(200 * time.Millisecond):
// 			t.Fatalf("[%d]: expected to receive result", i)
// 		}
// 	}

// 	conn.readDeadline = time.Time{}
// 	fuzzyDeadline := time.Now().Add(tcpRosReadTimeout)

// 	select {
// 	case request := <-conn.readRequest:
// 		if request != 4 {
// 			t.Fatalf("[%d]: expected to request 4 length bytes, got %d", i, request)
// 		}

// 		// Verify the deadline was set appropriately.
// 		fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
// 		if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
// 			t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
// 		}
// 		conn.readDeadline = time.Time{}

// 		conn.readResponse <- fakeConnReadResponse{
// 			Buf: testCase.bytes[:4],
// 			Err: nil,
// 		}

// 	case <-time.After(time.Second):
// 		t.Fatalf("[%d]: expected read request", i)
// 	}

// 	fuzzyDeadline = time.Now().Add(tcpRosReadTimeout)

// 	select {
// 	case request := <-conn.readRequest:
// 		if request != len(testCase.expected) {
// 			t.Fatalf("[%d]: expected to request %d length bytes, got %d", i, len(testCase.expected), request)
// 		}

// 		// Verify the deadline was set appropriately.
// 		fuzzyDelta := conn.readDeadline.Sub(fuzzyDeadline)
// 		if fuzzyDelta > fuzzyTimeTolerance || fuzzyDelta < -fuzzyTimeTolerance {
// 			t.Fatalf("[%d]: read deadline was not updated correctly, delta: %v", i, fuzzyDelta)
// 		}

// 		conn.readResponse <- fakeConnReadResponse{
// 			Buf: testCase.bytes[4:],
// 			Err: nil,
// 		}

// 	case <-time.After(time.Second):
// 		t.Fatalf("[%d]: expected read request", i)
// 	}

// 	select {
// 	case result := <-resultChan:
// 		if result.Err != nil {
// 			t.Fatalf("[%d]: unexpected error %v", i, result.Err)
// 		}
// 		if result.Buf == nil {
// 			t.Fatalf("[%d]: result buffer is nil", i)
// 		} else if string(result.Buf) != string(testCase.expected) {
// 			t.Fatalf("[%d]: buffer mismatch. Result: %v, expected: %v", i, result.Buf, testCase.expected)
// 		}
// 	case <-time.After(time.Second):
// 		t.Fatalf("[%d]: expected to receive result", i)
// 	}
// }
