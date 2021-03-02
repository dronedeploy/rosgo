package ros

import (
	"bytes"
	goContext "context"
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

// Fake dialer used to create test connections
type TCPRosDialerFake struct {
	conn net.Conn
	err  error
	uri  string
}

// Dial fake impelementation for testing.
func (d *TCPRosDialerFake) Dial(_ goContext.Context, uri string) (net.Conn, error) {
	d.uri = uri
	return d.conn, d.err
}

var _ TCPRosDialer = &TCPRosNetDialer{}

// Testing starts here.

// Use fake dial to ensure we dial the correct uri.
func TestSubscription_Dial_WithError(t *testing.T) {
	pubURI := "testuri:12345"
	subscription := newTestSubscription(pubURI)
	testDialer := &TCPRosDialerFake{
		conn: nil,
		err:  errors.New("connectionFail"),
		uri:  "",
	}
	subscription.dialer = testDialer

	logger := modular.NewRootLogger(logrus.New())
	log := logger.GetModuleLogger()

	ctx := newFakeContext()
	defer ctx.cleanUp()

	closed := make(chan struct{})
	go func() {
		subscription.run(ctx, &log)
		closed <- struct{}{}
	}()

	select {
	case <-closed:
		if testDialer.uri != pubURI {
			t.Fatalf("dialer was not sent the correct uri, got %s", testDialer.uri)
		}
		return
	case <-time.After(time.Second):
		t.Fatalf("took too long for client cancel dial")
	}
}

// Test to verify we can cancel the context on a hanging dial.
func TestSubscription_Dial_CanCancel(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	pubURI := l.Addr().String() // ensure we get a free port

	subscription := newTestSubscription(pubURI)

	logger := modular.NewRootLogger(logrus.New())
	log := logger.GetModuleLogger()

	ctx := newFakeContext()
	defer ctx.cleanUp()

	closed := make(chan struct{})
	go func() {
		subscription.run(ctx, &log)
		closed <- struct{}{}
	}()

	ctx.cancel()

	select {
	case <-closed:
		return
	case <-time.After(time.Second):
		t.Fatalf("took too long for client cancel dial")
	}
}

// Create a new subscription and pass headers still works when topic isn't provided by the publisher.
func TestSubscription_HeaderExchange_NoTopicIsOk(t *testing.T) {
	ctx, conn, subscription := createAndConnectToSubscription(t)
	defer ctx.cleanUp()
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
	select {
	case <-subscription.remoteDisconnectedChan:
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// Subscription closes the connection when it receives an invalid response header.
func TestSubscription_HeaderExchange_InvalidResponse(t *testing.T) {
	ctx, conn, subscription := createAndConnectToSubscription(t)
	defer ctx.cleanUp()
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
}

// Subscription closes when stop channel is closed during header exchange.
func TestSubscription_HeaderExchange_CloseRequestWithFrozenPublisher(t *testing.T) {

	ctx, conn, subscription := createAndConnectToSubscription(t)
	defer ctx.cleanUp()
	defer conn.Close()

	readAndVerifySubscriberHeader(t, conn, subscription.msgType)

	ctx.cancel()

	// Expect the Subscription has closed the connection.
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	dummySlice := make([]byte, 1)
	if _, err := conn.Read(dummySlice); err != io.EOF {
		t.Fatalf("expected subscription to close connection")
	}

	conn.Close()
}

// Valid messages are forwarded from the publisher TCP stream by the subscription.
func TestSubscription_ForwardsMessages(t *testing.T) {
	ctx, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer ctx.cleanUp()
	defer conn.Close()

	// Send something!
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x12, 0x23})

	// Send another one!
	sendMessageAndReceiveInChannel(t, conn, subscription.messageChan, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8})

	conn.Close()
	select {
	case channelName := <-subscription.remoteDisconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// Prioritizes new messages over old messages.
func TestSubscription_PrioritizesLatestMessage(t *testing.T) {
	ctx, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer ctx.cleanUp()
	defer conn.Close()

	staleBuffer := []byte{0x12, 0x23}
	newBuffer := []byte{0x1, 0x2, 0x3}

	sendMessageBytes(t, conn, staleBuffer)
	go sendMessageBytes(t, conn, newBuffer)
	<-time.After(1 * time.Millisecond)
	receiveMessageInChannel(t, subscription.messageChan, newBuffer)

	conn.Close()
	select {
	case channelName := <-subscription.remoteDisconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

// Drip feed a message
func TestSubscription_SubscriptionForwardsDripFedMessage(t *testing.T) {
	ctx, conn, subscription := createAndConnectSubscriptionToPublisher(t)
	defer ctx.cleanUp()
	defer conn.Close()

	// Send data through the drip feed.
	data := []byte{100: 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8}
	dripFeedMessageBytes(t, conn, data)
	receiveMessageInChannel(t, subscription.messageChan, data)

	conn.Close()
	select {
	case channelName := <-subscription.remoteDisconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

func TestSubscription_CancelAtStart(t *testing.T) {
	ctx, conn, _ := createAndConnectSubscriptionToPublisher(t)
	defer ctx.cleanUp()
	defer conn.Close()

	ctx.cancel() // Cancel the context.

	// Check the connection is closed by the subscription.
	buffer := make([]byte, 1)
	deadlineDuration := 5 * time.Millisecond
	conn.SetDeadline(time.Now().Add(deadlineDuration)) // Deadline stops this running forever if the connection wasn't closed.
	_, err := conn.Read(buffer)

	if err != io.EOF {
		t.Fatalf("Expected subscription to close connection, err: %s", err)
	}
}

func TestSubscription_CancelWithUnreadMessage(t *testing.T) {
	ctx, conn, _ := createAndConnectSubscriptionToPublisher(t)
	defer ctx.cleanUp()
	defer conn.Close()

	sendMessageBytes(t, conn, []byte{0x1, 0x2, 0x3})

	ctx.cancel() // Cancel the context.

	// Check the connection is closed by the subscription.
	buffer := make([]byte, 1)
	deadlineDuration := 5 * time.Millisecond
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
	remoteDisconnectedChan := make(chan string)
	msgType := testMessageType{topic}

	return newDefaultSubscription(
		pubURI, topic, msgType, nodeID,
		messageChan,
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
	<-time.After(50 * time.Millisecond)

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
	receiveMessageInChannel(t, msgChan, buffer)
}

// receiveMessageInChannel verifies that we receive the expected message.
func receiveMessageInChannel(t *testing.T, msgChan chan messageEvent, buffer []byte) {
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
	case <-time.After(time.Second):
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
		t.Fatalf("sendMessageBytes helper doesn't support more than 255 bytes!")
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

// sendMessageBytes sends a message payload as per TCPROS spec.
func dripFeedMessageBytes(t *testing.T, conn net.Conn, buffer []byte) {
	if len(buffer) > 255 {
		t.Fatalf("dripFeedMessageBytes helper doesn't support more than 255 bytes!")
	}

	// Packet structure is [ LENGTH<uint32> | PAYLOAD<bytes[LENGTH]> ].
	length := uint8(len(buffer))
	message := append([]byte{length, 0x00, 0x00, 0x00}, buffer...)
	for i := range message {
		n, err := conn.Write(message[:1])
		message = message[1:]
		if n != 1 || err != nil {
			t.Fatalf("[%d]: failed to drip feed message, n: %d : err: %s", i, n, err)
		}
		<-time.After(20 * time.Millisecond)
	}
}

// createAndConnectToSubscription creates a new subscription struct and prepares a TCP session.
func createAndConnectToSubscription(t *testing.T) (*fakeContext, net.Conn, *defaultSubscription) {
	pubConn, subConn := net.Pipe()
	pubURI := "fakeUri:12345"
	testDialer := &TCPRosDialerFake{
		conn: subConn,
		err:  nil,
		uri:  "",
	}

	subscription := newTestSubscription(pubURI)
	subscription.dialer = testDialer

	logger := modular.NewRootLogger(logrus.New())
	log := logger.GetModuleLogger()

	ctx := newFakeContext()
	subscription.startWithContext(ctx, &log)

	return ctx, pubConn, subscription
}

// createAndConnectSubscriptionToPublisher creates a new subscription struct and prepares a TCPROS session where we are ready to exchange messages.
func createAndConnectSubscriptionToPublisher(t *testing.T) (*fakeContext, net.Conn, *defaultSubscription) {
	ctx, conn, subscription := createAndConnectToSubscription(t)
	doHeaderExchangeAsPublisher(t, conn, subscription)

	return ctx, conn, subscription
}
