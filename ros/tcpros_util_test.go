package ros

import (
	goContext "context"
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
func (ctx *fakeContext) close(t *testing.T, d time.Duration) {
	select {
	case <-time.After(d):
		t.Fatalf("context did not close on receiver side")
	case ctx.done <- struct{}{}:
	}
}

// Helper for cleanup in a test.
func (ctx *fakeContext) cleanUp() {
	close(ctx.done)
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

type fakeConn struct {
	readBytes  []byte // Bytes to send on a read request
	readN      int    // Max bytes to read at once, beyond this is, it will return in chunks
	readErr    error  // Error to return on a read request
	writeBytes []byte // Bytes written from the last write request
	writeN     int    // Maximum allowable bytes to allow written
	writeError error  // Error to return on a write request
	localAddr  fakeAddr
	remoteAddr fakeAddr
}

func newFakeConn() *fakeConn {
	c := &fakeConn{}
	c.readBytes = make([]byte, 0)
	c.writeBytes = make([]byte, 0)
	c.localAddr = fakeAddr{"127.0.0.1:1"}
	c.remoteAddr = fakeAddr{"127.0.0.1:2"}
	return c
}

func (c *fakeConn) Read(b []byte) (n int, err error) {
	if len(c.readBytes) == 0 {
		return 0, c.readErr
	}
	nRead := intMin(c.readN, len(c.readBytes))
	if c.readN <= 0 {
		nRead = len(c.readBytes)
	}
	nRead = intMin(nRead, len(b))

	n = copy(b, c.readBytes[:nRead])
	c.readBytes = c.readBytes[n:]

	return n, c.readErr
}

func (c *fakeConn) Write(b []byte) (n int, err error) {
	nWrite := intMin(c.writeN, len(b))
	var appendBytes []byte
	n = copy(appendBytes, b[:nWrite])
	c.writeBytes = append(c.writeBytes, appendBytes...)

	return n, c.readErr
}

func (c *fakeConn) Close() error                       { return nil }
func (c *fakeConn) LocalAddr() net.Addr                { return &c.localAddr }
func (c *fakeConn) RemoteAddr() net.Addr               { return &c.remoteAddr }
func (c *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = &fakeConn{}
var _ io.Reader = &fakeConn{}

// Tests start here.

func Test_readTCPRosMessage_successfulCases(t *testing.T) {
	testCases := []struct {
		bytes    []byte
		expected []byte
	}{
		{ // Zero data case
			[]byte{0x00, 0x00, 0x00, 0x00},
			[]byte{},
		},
		{ // Has data case
			[]byte{0x02, 0x00, 0x00, 0x00, 'a', 'b'},
			[]byte{'a', 'b'},
		},
	}

	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()

	for i, testCase := range testCases {
		conn := newFakeConn()
		conn.readBytes = testCase.bytes
		go readTCPRosMessage(ctx, conn, resultChan)

		for {
			<-time.After(time.Millisecond)
			if len(conn.readBytes) == 0 {
				break
			}
		}

		select {
		case result := <-resultChan:
			if result.Err != nil {
				t.Fatalf("[%d]: unexepected error %v", i, result.Err)
			}
			if result.Buf == nil {
				t.Fatalf("[%d]: result buffer is nil", i)
			} else if string(result.Buf) != string(testCase.expected) {
				t.Fatalf("[%d]: buffer mismatch. Result: %v, expected: %v", i, result.Buf, testCase.expected)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("[%d]: expected to receive result", i)
		}
	}
	ctx.cleanUp()
}

func Test_readTCPRosMessage_whenCancelled_isSilent(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	conn := newFakeConn()

	go readTCPRosMessage(ctx, conn, resultChan)

	ctx.close(t, time.Second)

	select {
	case <-resultChan:
		t.Fatal("expected cancelled read to close silently")
	case <-time.After(200 * time.Millisecond):
	}
	ctx.cleanUp()
}

func Test_readTCPRosMessage_whenEoFError_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	conn := newFakeConn()
	conn.readErr = io.EOF
	go readTCPRosMessage(ctx, conn, resultChan)

	select {
	case result := <-resultChan:
		if result.Err != io.EOF {
			t.Fatalf("unexepected error %v", result.Err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected to receive conn error")
	}
	ctx.cleanUp()
}

func Test_readTCPRosMessage_whenTimeoutErrorDuringSize_returnsError(t *testing.T) {
	resultChan := make(chan TCPRosReadResult)
	ctx := newFakeContext()
	conn := newFakeConn()
	conn.readBytes = []byte{0x00, 0x00, 0x00} // One read short of getting size
	go readTCPRosMessage(ctx, conn, resultChan)

	for {
		<-time.After(time.Millisecond)
		if len(conn.readBytes) == 0 {
			break
		}
	}
	conn.readErr = os.ErrDeadlineExceeded

	select {
	case result := <-resultChan:
		if result.Err != os.ErrDeadlineExceeded {
			t.Fatalf("unexepected error %v", result.Err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected to receive conn error")
	}
	ctx.cleanUp()
}
