package ros

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	modular "github.com/edwinhayes/logrus-modular"
	"github.com/sirupsen/logrus"
)

// Helper structs.

// Set up testMessage fakes.
type testMessageType struct{ topic string }
type testMessage struct{}

// Ensure we satisfy the required interfaces.
var _ MessageType = testMessageType{}
var _ Message = testMessage{}

func (t testMessageType) Text() string {
	return t.topic
}

func (t testMessageType) MD5Sum() string {
	return "0123456789abcdeffedcba9876543210"
}

func (t testMessageType) Name() string {
	return "test_message"
}

func (t testMessageType) NewMessage() Message {
	return &testMessage{}
}

func (t testMessage) Type() MessageType {
	return &testMessageType{"test"}
}

func (t testMessage) Serialize(buf *bytes.Buffer) error {
	return nil
}

func (t testMessage) Deserialize(buf *bytes.Reader) error {
	return nil
}

// testReader provides the io.Reader interface.
type testReader struct {
	buffer []byte
	n      int
	err    error
}

// Ensure we satisfy the required interfaces.
var _ io.Reader = &testReader{}

func (r *testReader) Read(buf []byte) (n int, err error) {
	_ = copy(buf, r.buffer)
	n = r.n
	err = r.err
	return
}

// Testing starts here.

// Read size tests.

func TestSubscription_ReadSize(t *testing.T) {
	type testCase struct {
		buffer   []byte
		expected int
	}

	testCases := []testCase{
		{[]byte{0x00, 0x00, 0x00, 0x00}, 0},
		{[]byte{0x01, 0x00, 0x00, 0x00}, 1},
		{[]byte{0x0F, 0x00, 0x00, 0x00}, 15},
		{[]byte{0x00, 0x01, 0x00, 0x00}, 256},
		{[]byte{0xa1, 0x86, 0x01, 0x00}, 100001},
	}

	for _, tc := range testCases {
		reader := testReader{tc.buffer, 4, nil}
		n, res := readSize(&reader)
		if res != readOk {
			t.Fatalf("Expected read result %d, but got %d", readOk, res)
		}
		if n != tc.expected {
			t.Fatalf("ReadSize failed, expected %d, got %d", tc.expected, n)
		}

	}
}

// Read size error cases.
func TestSubscription_ReadSize_TooLarge(t *testing.T) {
	reader := testReader{[]byte{0x00, 0x00, 0x00, 0x80}, 4, nil}
	_, res := readSize(&reader)
	if res != readOutOfSync {
		t.Fatalf("Expected read result %d, but got %d", readOutOfSync, res)
	}
}

func TestSubscription_ReadSize_disconnected(t *testing.T) {
	reader := testReader{[]byte{}, 0, io.EOF}
	_, res := readSize(&reader)
	if res != remoteDisconnected {
		t.Fatalf("Expected read result %d, but got %d", remoteDisconnected, res)
	}
}

func TestSubscription_ReadSize_otherError(t *testing.T) {
	reader := testReader{[]byte{}, 0, errors.New("MysteryError")}
	_, res := readSize(&reader)
	if res != readFailed {
		t.Fatalf("Expected read result %d, but got %d", readFailed, res)
	}
}

// Read Raw Data tests.

// Verify basic buffer reading works correctly.
func TestSubscription_ReadRawData_ReadData(t *testing.T) {
	subscription := newTestSubscription("testUri")

	reader := testReader{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 10, nil}

	buf, res := subscription.readRawMessage(&reader, 4)
	if res != readOk {
		t.Fatalf("Expected read result %d, but got %d", readOk, res)
	}

	for i := 0; i < len(buf); i++ {
		if buf[i] != reader.buffer[i] {
			t.Fatalf("Expected read buf[%d] = %x, but got %x", i, reader.buffer[i], buf[i])
		}
	}
}

// Verify disconnection handling.
func TestSubscription_ReadRawData_disconnected(t *testing.T) {
	subscription := newTestSubscription("testUri")

	reader := testReader{[]byte{}, 0, io.EOF}

	_, res := subscription.readRawMessage(&reader, 4)
	if res != remoteDisconnected {
		t.Fatalf("Expected read result %d, but got %d", remoteDisconnected, res)
	}
}

// Create a new subscription and pass headers still works when topic isn't provided by the publisher.
func TestSubscription_HeaderExchange_NoTopicIsOk(t *testing.T) {
	l, conn, subscription := createAndConnectToSubscription(t)
	defer l.Close()
	defer conn.Close()

	readAndVerifySubscriberHeader(t, conn, subscription.msgType)

	replyHeader := []header{
		{"md5sum", subscription.msgType.MD5Sum()},
		{"type", subscription.msgType.Name()},
		{"callerid", "testPublisher"},
	}

	writeAndVerifyPublisherHeader(t, conn, subscription, replyHeader)

	// Expect that we store the topic anyway!
	if result, ok := subscription.event.ConnectionHeader["topic"]; ok {
		if subscription.topic != result {
			t.Fatalf("expected header[topic] = %s, but got %s", subscription.topic, result)
		}
	} else {
		t.Fatalf("subscription did not store header data for topic")
	}

	conn.Close()
	l.Close()
	select {
	case <-subscription.remoteDisconnectedChan:
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// Subscription closes the connection when it receives an invalid response header.
func TestSubscription_HeaderExchange_InvalidResponse(t *testing.T) {
	l, conn, subscription := createAndConnectToSubscription(t)
	defer l.Close()
	defer conn.Close()

	readAndVerifySubscriberHeader(t, conn, subscription.msgType)

	invalidMD5 := "00112233445566778899aabbccddeeff"
	replyHeader := []header{
		{"topic", subscription.topic},
		{"md5sum", invalidMD5},
		{"type", subscription.msgType.Name()},
		{"callerid", "testPublisher"},
	}

	if err := writeConnectionHeader(replyHeader, conn); err != nil {
		t.Fatalf("failed to write header: %s", replyHeader)
	}

	// Wait for the subscription to receive the data.
	<-time.After(time.Millisecond)

	// Expect the Subscription has closed the connection.
	dummySlice := make([]byte, 1)
	if _, err := conn.Read(dummySlice); err != io.EOF {
		t.Fatalf("expected subscription to close connection when receiving invalid header")

	}

	conn.Close()
	l.Close()
}

// Subscription closes when stop channel is closed during header exchange.
func TestSubscription_HeaderExchange_CloseRequestWithFrozenPublisher(t *testing.T) {
	t.Skip("TODO as part of RA-474")

	l, conn, subscription := createAndConnectToSubscription(t)
	defer l.Close()
	defer conn.Close()

	readAndVerifySubscriberHeader(t, conn, subscription.msgType)

	close(subscription.requestStopChan)

	// Expect the Subscription has closed the connection.
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	dummySlice := make([]byte, 1)
	if _, err := conn.Read(dummySlice); err != io.EOF {
		t.Fatalf("expected subscription to close connection")
	}

	conn.Close()
	l.Close()
}

// Valid messages are forwarded from the publisher TCP stream by the subscription.
func TestSubscription_SubscriptionForwardsMessages(t *testing.T) {
	l, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer l.Close()
	defer conn.Close()

	// Send something!
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x12, 0x23})

	// Send another one!
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8})

	conn.Close()
	l.Close()
	select {
	case channelName := <-subscription.remoteDisconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// The subscription adheres to the flow control policy.
func TestSubscription_FlowControl(t *testing.T) {
	l, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer conn.Close()
	defer l.Close()

	// Send something - channel enabled.
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x12, 0x23})

	// Send something - channel disabled.
	subscription.enableChan <- false
	sendMessageNoReceive(t, conn, subscription.messageChan, []byte{0x12, 0x23})

	// Send something - channel enabled.
	subscription.enableChan <- true
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x12, 0x23})
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x12, 0x23, 0x43})

	// Send something - channel disabled.
	subscription.enableChan <- false
	sendMessageNoReceive(t, conn, subscription.messageChan, []byte{0x12, 0x23})
	sendMessageNoReceive(t, conn, subscription.messageChan, []byte{0x12, 0x23, 0x43})

	conn.Close()
	l.Close()
	select {
	case channelName := <-subscription.remoteDisconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// Request stop shuts down an active connection.
func TestSubscription_RequestStop(t *testing.T) {
	l, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer l.Close()
	defer conn.Close()

	// Close the stop channel. Expect this to shutdown the subscription.
	close(subscription.requestStopChan)

	// Check the connection is closed by the subscription.
	buffer := make([]byte, 1)
	deadlineDuration := 5 * time.Second                // Subscriptions only check the stop request every second, so our close check needs to be longer.
	conn.SetDeadline(time.Now().Add(deadlineDuration)) // Deadline stops this running forever if the connection wasn't closed.
	_, err := conn.Read(buffer)

	if err != io.EOF {
		t.Fatalf("Expected subscription to close connection, err: %s", err)
	}
}

// Private Helper functions.

// Create a test subscription object.
func newTestSubscription(pubURI string) *defaultSubscription {

	topic := "/test/topic"
	nodeID := "testNode"
	messageChan := make(chan messageEvent)
	requestStopChan := make(chan struct{})
	remoteDisconnectedChan := make(chan string)
	enableChan := make(chan bool)
	msgType := testMessageType{topic}

	return newDefaultSubscription(
		pubURI, topic, msgType, nodeID,
		messageChan,
		enableChan,
		requestStopChan,
		remoteDisconnectedChan)
}

// doHeaderExchange emulates the header exchange as a service server. Puts the client in a state where it is ready to send a request.
func doHeaderExchangeAsPublisher(t *testing.T, conn net.Conn, subscription *defaultSubscription) {
	readAndVerifySubscriberHeader(t, conn, subscription.msgType)
	replyHeader := []header{
		{"topic", subscription.topic},
		{"md5sum", subscription.msgType.MD5Sum()},
		{"type", subscription.msgType.Name()},
		{"callerid", "testPublisher"},
	}
	writeAndVerifyPublisherHeader(t, conn, subscription, replyHeader)
}

// readAndVerifySubscriberHeader reads the incoming header from the subscriber, and verifies header contents.
func readAndVerifySubscriberHeader(t *testing.T, conn net.Conn, msgType MessageType) {
	resHeaders, err := readConnectionHeader(conn)
	if err != nil {
		t.Fatal("Failed to read header:", err)
	}

	resHeaderMap := make(map[string]string)
	for _, h := range resHeaders {
		resHeaderMap[h.key] = h.value
	}

	if resHeaderMap["md5sum"] != msgType.MD5Sum() {
		t.Fatalf("incorrect MD5 sum %s", resHeaderMap["md5sum"])
	}

	if resHeaderMap["topic"] != msgType.Text() {
		t.Fatalf("incorrect topic: %s", msgType.Text())
	}

	if resHeaderMap["type"] != msgType.Name() {
		t.Fatalf("incorrect type: %s", resHeaderMap["type"])
	}

	if resHeaderMap["callerid"] != "testNode" {
		t.Fatalf("incorrect caller ID: %s", resHeaderMap["testNode"])
	}
}

// writeAndVerifyPublisherHeader writes a header to the subscriber; verifies the subscriber captures the header correctly.
func writeAndVerifyPublisherHeader(t *testing.T, conn net.Conn, subscription *defaultSubscription, replyHeader []header) {

	if err := writeConnectionHeader(replyHeader, conn); err != nil {
		t.Fatalf("Failed to write header: %s", replyHeader)
	}

	// Wait for the subscription to receive the data.
	<-time.After(time.Millisecond)

	for _, expected := range replyHeader {
		if result, ok := subscription.event.ConnectionHeader[expected.key]; ok {
			if expected.value != result {
				t.Fatalf("expected header[%s] = %s, but got %s", expected.key, expected.value, result)
			}
		} else {
			t.Fatalf("subscription did not store header data for %s", expected.key)
		}
	}
}

// sendMessageAndReceiveInChannel sends a message which we expect is passed on by the subscription.
func sendMessageAndReceiveInChannel(t *testing.T, conn net.Conn, msgChan chan messageEvent, buffer []byte) {
	sendMessageBytes(t, conn, buffer)

	select {
	case message := <-msgChan:

		if message.event.PublisherName != "testPublisher" {
			t.Fatalf("Published with the wrong publisher name: %s", message.event.PublisherName)
		}
		if len(message.bytes) != len(buffer) {
			t.Fatalf("Payload size is incorrect: %d, expected: %d", len(message.bytes), len(buffer))
		}
		for i := 1; i < len(buffer); i++ {
			if message.bytes[i] != buffer[i] {
				t.Fatalf("message.bytes[%d] = %x, expected %x", i, message.bytes[i], buffer[i])
			}
		}
		return
	case <-time.After(time.Duration(10) * time.Millisecond):
		t.Fatalf("Did not receive message from channel")
	}
}

// sendMessageNoReceive sends a message which we expect is dropped by the subscription.
func sendMessageNoReceive(t *testing.T, conn net.Conn, msgChan chan messageEvent, buffer []byte) {
	sendMessageBytes(t, conn, buffer)

	select {
	case _ = <-msgChan:
		t.Fatalf("Message channel sent bytes; should be disabled!")
	case <-time.After(time.Duration(50) * time.Millisecond):
		return
	}
}

// sendMessageBytes sends a message payload as per TCPROS spec.
func sendMessageBytes(t *testing.T, conn net.Conn, buffer []byte) {
	if len(buffer) > 255 {
		t.Fatalf("sendMessageAndReceiveInChannel helper doesn't support more than 255 bytes!")
	}

	// Packet structure is [ LENGTH<uint32> | PAYLOAD<bytes[LENGTH]> ].
	length := uint8(len(buffer))
	n, err := conn.Write([]byte{length, 0x00, 0x00, 0x00})
	if n != 4 || err != nil {
		t.Fatalf("Failed to write message size, n: %d : err: %s", n, err)
	}
	n, err = conn.Write(buffer) // payload
	if n != len(buffer) || err != nil {
		t.Fatalf("Failed to write message payload, n: %d : err: %s", n, err)
	}
}

// createAndConnectToSubscription creates a new subscription struct and prepares a TCPROS session where we are ready to exchange messages.
func createAndConnectToSubscription(t *testing.T) (net.Listener, net.Conn, *defaultSubscription) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	pubURI := l.Addr().String()

	subscription := newTestSubscription(pubURI)

	logger := modular.NewRootLogger(logrus.New())
	log := logger.GetModuleLogger()

	subscription.start(&log)

	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	return l, conn, subscription
}

// createAndConnectSubscriptionToPublisher creates a new subscription struct and prepares a TCPROS session where we are ready to exchange messages.
func createAndConnectSubscriptionToPublisher(t *testing.T) (net.Listener, net.Conn, *defaultSubscription) {
	l, conn, subscription := createAndConnectToSubscription(t)
	doHeaderExchangeAsPublisher(t, conn, subscription)

	return l, conn, subscription
}
