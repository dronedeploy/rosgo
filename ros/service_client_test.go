package ros

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	modular "github.com/edwinhayes/logrus-modular"
	"github.com/sirupsen/logrus"
)

// Fake Service types for testing.

// Set up testRequestMessage fakes.
type testRequestMessageType struct{}
type testRequestMessage struct {
	err error
}

// Ensure we satisfy the required interfaces.
var _ MessageType = testRequestMessageType{}
var _ Message = testRequestMessage{}

func (t testRequestMessageType) Text() string {
	return "test_request_type"
}

func (t testRequestMessageType) MD5Sum() string {
	return "0123456789abcdeffedcba9876543210"
}

func (t testRequestMessageType) Name() string {
	return "test_request"
}

func (t testRequestMessageType) NewMessage() Message {
	return &testRequestMessage{}
}

func (m testRequestMessage) Type() MessageType {
	return &testRequestMessageType{}
}

func (m testRequestMessage) Serialize(buf *bytes.Buffer) error {
	buf.WriteString("Request")
	return m.err
}

func (m testRequestMessage) Deserialize(buf *bytes.Reader) error {
	return m.err
}

// Set up testResponseMessage fakes.
type testResponseMessageType struct{}
type testResponseMessage struct {
	err error
}

// Ensure we satisfy the required interfaces.
var _ MessageType = testResponseMessageType{}
var _ Message = testResponseMessage{}

func (t testResponseMessageType) Text() string {
	return "test_response_type"
}

func (t testResponseMessageType) MD5Sum() string {
	return "0123456789abcdeffedcba9876543210"
}

func (t testResponseMessageType) Name() string {
	return "test_response"
}

func (t testResponseMessageType) NewMessage() Message {
	return &testResponseMessage{}
}

func (m testResponseMessage) Type() MessageType {
	return &testResponseMessageType{}
}

func (m testResponseMessage) Serialize(buf *bytes.Buffer) error {
	buf.WriteString("Response")
	return m.err
}

func (m testResponseMessage) Deserialize(buf *bytes.Reader) error {
	return m.err
}

type testServiceType struct{}
type testService struct {
	requestErr  error // Injects an error into the serialize/deserialize methods of its request messages.
	responseErr error // Injects an error into the serialize/deserialize methods of its response messages.
}

// Ensure we satisfy the required interfaces.
var _ ServiceType = testServiceType{}
var _ Service = testService{}

func (t testServiceType) MD5Sum() string {
	return "0123456789abcdeffedcba9876543210"
}

func (t testServiceType) Name() string {
	return "test_service"
}

func (t testServiceType) RequestType() MessageType {
	return &testRequestMessageType{}
}

func (t testServiceType) ResponseType() MessageType {
	return &testResponseMessageType{}
}

func (t testServiceType) NewService() Service {
	return &testService{}
}

func (s testService) ReqMessage() Message {
	return &testRequestMessage{s.requestErr}
}

func (s testService) ResMessage() Message {
	return &testResponseMessage{s.responseErr}
}

// Tests

func TestServiceClient_SuccessfulServiceExchange(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)
	doSendOk(t, conn, true)
	doSendResponse(t, conn)

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long for client to stop")
	case err := <-result:
		if err != nil {
			t.Fatalf("expected successful request/response, got error %s", err)
		}
	}
}

func TestServiceClient_HangUpError_HeaderExchange(t *testing.T) {
	l, conn, _, result := setupServiceServerAndClient(t)

	conn.Close()
	l.Close()

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long to shutdown test client")
	case err := <-result:
		if err == nil {
			t.Fatal("expected error from early hang up, got no error")
		}
	}
}

func TestServiceClient_HangUpError_Ok(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)

	conn.Close()
	l.Close()

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long to shutdown test client")
	case err := <-result:
		if err == nil {
			t.Fatal("expected error from early hang up, got no error")
		}
	}
}

func TestServiceClient_HangUpError_Response(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)
	doSendOk(t, conn, true)

	conn.Close()
	l.Close()

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long to shutdown test client")
	case err := <-result:
		if err == nil {
			t.Fatal("expected error from early hang up, got no error")
		}
	}
}

func TestServiceClient_NotOkError(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)
	doSendOk(t, conn, false)
	doSendResponse(t, conn)

	conn.Close()
	l.Close()

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long to shutdown test client")
	case err := <-result:
		if err == nil {
			t.Fatal("expected error from not ok, got no error")
		}
	}
}

func TestServiceClient_SuccessfulServiceExchange_HeaderDelay(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doReadConnectionHeader(t, conn)
	<-time.After(500 * time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("premature exit due to header response delay, got error %s", err)
	default:
	}
	doWriteConnectionHeader(t, conn, client)
	doReceiveRequest(t, conn)
	doSendOk(t, conn, true)
	doSendResponse(t, conn)

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long for client to stop")
	case err := <-result:
		if err != nil {
			t.Fatalf("expected successful request/response, got error %s", err)
		}
	}
}

func TestServiceClient_SuccessfulServiceExchange_OkDelay(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)
	<-time.After(500 * time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("premature exit due to ok reply delay, got error %s", err)
	default:
	}
	doSendOk(t, conn, true)
	doSendResponse(t, conn)

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long for client to stop")
	case err := <-result:
		if err != nil {
			t.Fatalf("expected successful request/response, got error %s", err)
		}
	}
}

func TestServiceClient_SuccessfulServiceExchange_ResponseDelay(t *testing.T) {
	l, conn, client, result := setupServiceServerAndClient(t)
	defer l.Close()
	defer conn.Close()

	doServiceServerHeaderExchange(t, conn, client)
	doReceiveRequest(t, conn)
	doSendOk(t, conn, true)
	<-time.After(500 * time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("premature exit due to response delay, got error %s", err)
	default:
	}
	doSendResponse(t, conn)

	select {
	case <-time.After(time.Second):
		t.Fatal("took too long for client to stop")
	case err := <-result:
		if err != nil {
			t.Fatalf("expected successful request/response, got error %s", err)
		}
	}
}

// Test helper functions.

func doReadConnectionHeader(t *testing.T, conn net.Conn) {
	if _, err := readConnectionHeader(conn); err != nil {
		t.Fatal("Failed to read header:", err)
	}
}

func doWriteConnectionHeader(t *testing.T, conn net.Conn, client *defaultServiceClient) {
	replyHeader := []header{
		{"service", client.service},
		{"md5sum", client.srvType.MD5Sum()},
		{"type", client.srvType.Name()},
		{"callerid", "testServer"},
	}

	if err := writeConnectionHeader(replyHeader, conn); err != nil {
		t.Fatalf("Failed to write header: %s", replyHeader)
	}
}

// doHeaderExchange emulates the header exchange as a service server. Puts the client in a state where it is ready to send a request.
func doServiceServerHeaderExchange(t *testing.T, conn net.Conn, client *defaultServiceClient) {
	doReadConnectionHeader(t, conn)
	doWriteConnectionHeader(t, conn, client)
}

// Receives a request message. Because we are using test messages, we know this should be `Receive` serialized.
func doReceiveRequest(t *testing.T, conn net.Conn) {
	// Receive a request.
	var size uint32
	if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
		t.Fatalf("failed to read request size, %s", err)
	}

	if size != 7 {
		t.Fatalf("expected size 7, got %d", size)
	}

	buffer := make([]byte, 7) // Expect `Request`.
	n, err := conn.Read(buffer)

	if err != nil {
		t.Fatalf("received error instead of request: %s", err)
	}
	if n != 7 {
		t.Fatalf("expected to read 7 bytes, got %d", n)
	}
	if string(buffer) != "Request" {
		t.Fatalf("request serialized to unexpected bytes, expected `Request` got %s", string(buffer[4:]))
	}
}

// Sends an OK byte
func doSendOk(t *testing.T, conn net.Conn, isOk bool) {
	var ok uint8
	if isOk {
		ok = 1
	} else {
		ok = 0
	}

	// Reply with Ok byte.
	if err := binary.Write(conn, binary.LittleEndian, &ok); err != nil {
		t.Fatalf("failed to write response size, %s", err)
	}
}

// Sends a response message which is just `Response` serialized.
func doSendResponse(t *testing.T, conn net.Conn) {
	size := uint32(8)
	buffer := []byte("Response")

	// Send a response.
	if err := binary.Write(conn, binary.LittleEndian, &size); err != nil {
		t.Fatalf("failed to write response size, %s", err)
	}
	n, err := conn.Write(buffer)
	if err != nil {
		t.Fatalf("failed to write response, error: %s", err)
	}
	if n != 8 {
		t.Fatalf("expected to write 8 bytes, got %d", n)
	}
}

// setupServiceServer establishes all init values
func setupServiceServerAndClient(t *testing.T) (net.Listener, net.Conn, *defaultServiceClient, chan error) {
	rootLogger := modular.NewRootLogger(logrus.New())

	logger := rootLogger.GetModuleLogger()
	logger.SetLevel(logrus.DebugLevel)

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	serviceURI := l.Addr().String()

	client := &defaultServiceClient{
		logger:    &logger,
		service:   "/test/service",
		srvType:   testServiceType{},
		masterURI: "",
		nodeID:    "testNode",
	}

	result := make(chan error)
	go func() {
		err := client.doServiceRequest(testService{}, serviceURI)
		result <- err
	}()

	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	return l, conn, client, result
}
