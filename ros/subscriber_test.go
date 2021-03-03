package ros

import (
	"io"
	"net"
	"testing"
	"time"

	modular "github.com/edwinhayes/logrus-modular"
	"github.com/sirupsen/logrus"
	gengo "github.com/team-rocos/rosgo/libgengo"
)

// `subscriber_test.go` uses `testMessageType` and `testMessage` defined in `subscription_test.go`.

// SubscriberRosAPI implements SubscriberRos using callRosAPI rpc calls.
type fakeSubscriberRos struct {
	pub           []string
	uri           string
	uriErr        error
	unregistered  bool
	unregisterErr error
}

func newFakeSubscriberRos() *fakeSubscriberRos {
	a := &fakeSubscriberRos{}
	a.pub = make([]string, 0)
	a.uri = ""
	a.uriErr = nil
	a.unregistered = false
	a.unregisterErr = nil
	return a
}

// RequestTopicURI requests the URI of a given topic from a publisher.
func (a *fakeSubscriberRos) RequestTopicURI(pub string) (string, error) {
	a.pub = append(a.pub, pub)
	return a.uri, a.uriErr
}

// Unregister removes a subscriber from a topic.
func (a *fakeSubscriberRos) Unregister() error {
	a.unregistered = true
	return a.unregisterErr
}

var _ SubscriberRos = &SubscriberRosAPI{}

func TestRemotePublisherConn_DoesConnect(t *testing.T) {
	topic := "/test/topic"
	msgType := testMessageType{topic}

	ctx, conn, _, disconnectedChan := setupRemotePublisherConnTest(t)
	defer ctx.cleanUp()
	defer conn.Close()

	readAndVerifySubscriberHeader(t, conn, msgType) // Test helper from subscription_test.go.

	replyHeader := []header{
		{"topic", topic},
		{"md5sum", msgType.MD5Sum()},
		{"type", msgType.Name()},
		{"callerid", "testPublisher"},
	}

	err := writeConnectionHeader(replyHeader, conn)
	if err != nil {
		t.Fatalf("Failed to write header: %s", replyHeader)
	}

	conn.Close()
	select {
	case <-disconnectedChan:
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

func TestRemotePublisherConn_ClosesFromContext(t *testing.T) {

	ctx, conn, _, _ := setupRemotePublisherConnTest(t)
	defer ctx.cleanUp()
	defer conn.Close()

	connectToSubscriber(t, conn)
	<-time.After(time.Duration(50 * time.Millisecond))

	// Signal to close.
	ctx.cancel()
	<-time.After(time.Duration(50 * time.Millisecond))

	// Check that buffer closed.
	buffer := make([]byte, 1)
	conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := conn.Read(buffer)

	if err != io.EOF {
		t.Fatalf("Expected subscriber to close connection")
	}
}

func TestRemotePublisherConn_RemoteReceivesData(t *testing.T) {

	ctx, conn, msgChan, disconnectedChan := setupRemotePublisherConnTest(t)
	defer ctx.cleanUp()
	defer conn.Close()

	connectToSubscriber(t, conn)

	// Send something!
	sendMessageAndReceiveInChannel(t, conn, msgChan, []byte{0x12, 0x23})

	// Send another one!
	sendMessageAndReceiveInChannel(t, conn, msgChan, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8})

	conn.Close()
	select {
	case channelName := <-disconnectedChan:
		t.Log(channelName)
		return
	case <-time.After(time.Duration(100) * time.Millisecond):
		t.Fatalf("Took too long for client to disconnect from publisher")
	}
}

func TestSubscriber_Run_Shutdown(t *testing.T) {
	fields := []gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", true, 8),
	}
	msgType := &DynamicMessageType{
		spec:         generateTestSpec(fields), // From dynamic_message_tests.go.
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}
	sub := newDefaultSubscriber("testTopic", msgType, func() {})
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	nodeID := "testNode"
	logger := modular.NewRootLogger(logrus.New())
	log := logger.GetModuleLogger()
	log.SetLevel(logrus.InfoLevel)

	shutdownSubscriber := make(chan struct{})
	go func() {
		sub.run(ctx, jobChan, enableChan, rosAPI, nodeID, &log)
		shutdownSubscriber <- struct{}{}
	}()

	shutdownCaller := make(chan struct{})
	go func() {
		sub.Shutdown()
		shutdownCaller <- struct{}{}
	}()

	select {
	case <-shutdownSubscriber:
	case <-time.After(time.Second):
		t.Fatal("shutdown subscriber failed")
	}

	select {
	case <-shutdownCaller:
	case <-time.After(time.Second):
		t.Fatal("shutdown response failed")
	}

	if rosAPI.unregistered == false {
		t.Fatal("did not unregister on shutdown")
	}
}

// setupRemotePublisherConnTest establishes all init values and kicks off the start function.
func setupRemotePublisherConnTest(t *testing.T) (*fakeContext, net.Conn, chan messageEvent, chan string) {
	pubConn, subConn := net.Pipe()
	pubURI := "fakeUri:12345"
	testDialer := &TCPRosDialerFake{
		conn: subConn,
		err:  nil,
		uri:  "",
	}
	ctx := newFakeContext()

	logger := modular.NewRootLogger(logrus.New())
	topic := "/test/topic"
	nodeID := "testNode"
	msgChan := make(chan messageEvent)
	disconnectedChan := make(chan string)
	msgType := testMessageType{}

	log := logger.GetModuleLogger()
	log.SetLevel(logrus.InfoLevel)

	startRemotePublisherConn(ctx, &log, testDialer, pubURI, topic, msgType, nodeID, msgChan, disconnectedChan)

	return ctx, pubConn, msgChan, disconnectedChan
}

// connectToSubscriber connects a net.Conn object to a subscriber and emulates the publisher header exchange. Puts the subscriber in a state where it is ready to receive messages.
func connectToSubscriber(t *testing.T, conn net.Conn) {
	msgType := testMessageType{}
	topic := "/test/topic"

	_, err := readConnectionHeader(conn)

	if err != nil {
		t.Fatal("Failed to read header:", err)
	}

	replyHeader := []header{
		{"topic", topic},
		{"md5sum", msgType.MD5Sum()},
		{"type", msgType.Name()},
		{"callerid", "testPublisher"},
	}

	err = writeConnectionHeader(replyHeader, conn)
	if err != nil {
		t.Fatalf("Failed to write header: %s", replyHeader)
	}
}
