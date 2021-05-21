package ros

import (
	goContext "context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/team-rocos/go-common/logging"

	gengo "github.com/team-rocos/rosgo/libgengo"
)

// `subscriber_test.go` uses `testMessageType` and `testMessage` defined in `subscription_test.go`.

// SubscriberRosAPI implements SubscriberRos using callRosAPI rpc calls.
type fakeSubscriberRos struct {
	delay         time.Duration
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
	a.delay = 0
	return a
}

// RequestTopicURI requests the URI of a given topic from a publisher.
func (a *fakeSubscriberRos) RequestTopicURI(pub string) (string, error) {
	a.pub = append(a.pub, pub)
	if a.delay != 0 {
		<-time.After(a.delay)
	}
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
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}
	log := makeTestLogger()

	shutdownSubscriber := make(chan struct{})
	go func() {
		sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
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

	<-time.After(10 * time.Millisecond) // Give the unregister call a moment.
	if rosAPI.unregistered == false {
		t.Fatal("did not unregister on shutdown")
	}
}

func TestSubscriber_Run_FlowControl(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	sub.msgChan <- messageEvent{
		bytes: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case <-jobChan:
	case <-time.After(time.Second):
		t.Fatal("expected job from message channel")
	}

	enableChan <- false

	sub.msgChan <- messageEvent{
		bytes: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case <-jobChan:
		t.Fatal("job received on disabled channel")
	case <-time.After(5 * time.Millisecond):
	}

	enableChan <- true

	sub.msgChan <- messageEvent{
		bytes: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case <-jobChan:
	case <-time.After(time.Second):
		t.Fatal("expected job from enabled channel")
	}
}

func TestSubscriber_Run_JobPackaging(t *testing.T) {
	var msg Message
	var event MessageEvent
	called := false
	sub := makeTestSubscriberWithJobCallback(func(m Message, e MessageEvent) {
		msg = m
		event = e
		called = true
	})
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	bPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	sub.msgChan <- messageEvent{
		bytes: bPayload,
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case job := <-jobChan:
		job()
	case <-time.After(time.Second):
		t.Fatal("expected job from message channel")
	}

	if called == false {
		t.Fatal("Job callback not executed")
	}

	if dmsg, ok := msg.(*DynamicMessage); ok {
		if b, ok := dmsg.data["u8"]; ok {
			if bArray, ok := b.([]byte); ok {
				if string(bArray) != string(bPayload) {
					t.Fatalf("payload mismatch, expected %v, got %v", bPayload, bArray)
				}
			} else {
				t.Fatal("expected u8 field to be byte array")
			}
		} else {
			t.Fatal("expected u8 field in message data")
		}
	} else {
		t.Fatal("message was not dynamic message")
	}

	if event.PublisherName != "TestPublisher" {
		t.Fatal("invalid message event")
	}
}

func TestSubscriber_Run_JobCancellation(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	shutdownSubscriber := make(chan struct{})
	go func() {
		sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
		shutdownSubscriber <- struct{}{}
	}()

	bPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	sub.msgChan <- messageEvent{
		bytes: bPayload,
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	go sub.Shutdown()

	select {
	case <-shutdownSubscriber:
	case <-time.After(time.Second):
		t.Fatal("expected shutdown")
	}

	// Do some cleanup just in case.
	done := false
	for done == false {
		select {
		case <-jobChan:
		default:
			done = true
		}
	}
}

func TestSubscriber_Run_JobDisable(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	bPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	sub.msgChan <- messageEvent{
		bytes: bPayload,
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	enableChan <- false // Disable flow control before we fetch the job.

	select {
	case <-jobChan:
		t.Fatalf("expected to disable latest job")
	case <-time.After(time.Millisecond):
	}
}

func TestSubscriber_Run_JobCallbacks(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	// Send new callback functions to subscription.
	var event MessageEvent
	cb1 := func(m Message, e MessageEvent) {
		event = e
	}
	cb2Called := false
	cb2 := func(m Message, e MessageEvent) {
		cb2Called = true
	}

	sub.addCallbackChan <- cb1
	sub.addCallbackChan <- cb2

	bPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	sub.msgChan <- messageEvent{
		bytes: bPayload,
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case job := <-jobChan:
		job()
	case <-time.After(time.Second):
		t.Fatalf("expected to receive job")
	}

	if event.PublisherName != "TestPublisher" {
		t.Fatal("cb1 not called when executing job")
	}
	if cb2Called == false {
		t.Fatal("cb2 not called when executing job")
	}
}

func TestSubscriber_Run_JobCallbacks_BadDeserialization(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	// Send new callback functions to subscription.
	cbCalled := false
	cb := func(m Message, e MessageEvent) {
		cbCalled = true
	}
	sub.addCallbackChan <- cb

	// Bad payload specification (expect u8[8], but send u8[7])
	bPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06}
	sub.msgChan <- messageEvent{
		bytes: bPayload,
		event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
	}

	select {
	case job := <-jobChan:
		job()
	case <-time.After(time.Second):
		t.Fatalf("expected to receive job")
	}

	if cbCalled == true {
		t.Fatal("exected callback for invalid dynamic message")
	}
}

func TestSubscriber_Run_JobPrioritization(t *testing.T) {
	var msg Message
	sub := makeTestSubscriberWithJobCallback(func(m Message, e MessageEvent) {
		msg = m
	})
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	stalePayload := []byte{0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7}
	newPayload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

	go func() {
		sub.msgChan <- messageEvent{
			bytes: stalePayload,
			event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
		}

		sub.msgChan <- messageEvent{
			bytes: newPayload,
			event: MessageEvent{"TestPublisher", time.Now(), make(map[string]string)},
		}
	}()

	// Delay so both messages get picked up.
	<-time.After(5 * time.Millisecond)

	select {
	case job := <-jobChan:
		job()
	case <-time.After(time.Second):
		t.Fatal("expected job from message channel")
	}

	if dmsg, ok := msg.(*DynamicMessage); ok {
		if b, ok := dmsg.data["u8"]; ok {
			if bArray, ok := b.([]byte); ok {
				if string(bArray) != string(newPayload) {
					t.Fatalf("payload mismatch, expected %v, got %v", newPayload, bArray)
				}
			} else {
				t.Fatal("expected u8 field to be byte array")
			}
		} else {
			t.Fatal("expected u8 field in message data")
		}
	} else {
		t.Fatal("message was not dynamic message")
	}

	// Do some cleanup just in case.
	done := false
	for done == false {
		select {
		case <-sub.msgChan:
		default:
			done = true
		}
	}
}

func TestSubscriber_Run_Publishers(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	rosAPI.uri = "fakeURI"
	log := makeTestLogger()
	pubURIs := []string{}
	subscriptionContext := []goContext.Context{}
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {
		pubURIs = append(pubURIs, pubURI)
		subscriptionContext = append(subscriptionContext, ctx)
	}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)

	// Send publishers.
	pubSend := []string{"pub1", "pub2"}
	sub.pubListChan <- pubSend

	// Delay so publisher list is picked up.
	<-time.After(20 * time.Millisecond)

	if reflect.DeepEqual(sub.pubList, pubSend) == false {
		t.Fatalf("expected pubList to match sent publishers, got %v", sub.pubList)
	}
	if reflect.DeepEqual(pubURIs, []string{"fakeURI", "fakeURI"}) == false {
		t.Fatalf("expected startSubscriptions to receive URIs, got %v", pubURIs)
	}

	// Remove second publisher, add a third publisher.
	rosAPI.uri = "fakeURI3"
	pubSend = []string{"pub1", "pub3"}
	sub.pubListChan <- pubSend

	// Delay so publisher list is picked up.
	<-time.After(20 * time.Millisecond)

	if reflect.DeepEqual(sub.pubList, pubSend) == false {
		t.Fatalf("expected pubList to match sent publishers, got %v", sub.pubList)
	}
	if reflect.DeepEqual(pubURIs, []string{"fakeURI", "fakeURI", "fakeURI3"}) == false {
		t.Fatalf("expected startSubscriptions to receive URIs, got %v", pubURIs)
	}
	// Check second context is cancelled.
	select {
	case <-subscriptionContext[0].Done():
		t.Fatalf("unexpected cancellation of subscription 1")
	case <-subscriptionContext[1].Done():
	case <-subscriptionContext[2].Done():
		t.Fatalf("unexpected cancellation of subscription 3")
	default:
		t.Fatalf("expected cancellation of subscription 2")
	}

	// Make subscription 3 report it has disconnected.
	sub.disconnectedChan <- "fakeURI3"
	<-time.After(20 * time.Millisecond)

	// Check third context is cancelled.
	select {
	case <-subscriptionContext[0].Done():
		t.Fatalf("unexpected cancellation of subscription 1")
	case <-subscriptionContext[2].Done():
	default:
		t.Fatalf("expected cancellation of subscription 3")
	}

	if reflect.DeepEqual(sub.pubList, []string{"pub1"}) == false {
		t.Fatalf("expected pubList to match current publishers, got %v", sub.pubList)
	}

	// Add pub3 back again.
	sub.pubListChan <- pubSend

	// Delay so publisher list is picked up.
	<-time.After(20 * time.Millisecond)

	if reflect.DeepEqual(sub.pubList, pubSend) == false {
		t.Fatalf("expected pubList to match sent publishers, got %v", sub.pubList)
	}
	if reflect.DeepEqual(pubURIs, []string{"fakeURI", "fakeURI", "fakeURI3", "fakeURI3"}) == false {
		t.Fatalf("expected startSubscriptions to receive URIs, got %v", pubURIs)
	}

	// Shutdown the subscription - checks that all subscriptions are cancelled.
	sub.Shutdown()

	// Give deferred cancels a chance to trigger.
	<-time.After(20 * time.Millisecond)

	// Check first context is cancelled.
	select {
	case <-subscriptionContext[0].Done():
	default:
		t.Fatalf("expected cancellation of subscription 1")
	}

	// Check third context is cancelled.
	select {
	case <-subscriptionContext[3].Done():
	default:
		t.Fatalf("expected cancellation of subscription 3")
	}
}

func TestSubscriber_Run_AddPublishersDontBlock(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	rosAPI.uri = "fakeURI"
	rosAPI.delay = 100 * time.Millisecond
	log := makeTestLogger()
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {
	}

	shutdownSubscriber := make(chan struct{})
	go func() {
		sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
		shutdownSubscriber <- struct{}{}
	}()

	// Send publishers.
	pubSend := []string{"pub1", "pub2"}
	sub.pubListChan <- pubSend

	// Shutdown the subscription - checks that all subscriptions are cancelled.
	go sub.Shutdown()

	// Check first context is cancelled.
	select {
	case <-shutdownSubscriber:
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("expected subscriber to shutdown")
	}
}

func TestSubscriber_Run_AddPublisherOverride(t *testing.T) {
	sub := makeTestSubscriber()
	ctx := newFakeContext()
	jobChan := make(chan func())
	enableChan := make(chan bool)
	rosAPI := newFakeSubscriberRos()
	rosAPI.uri = "fakeURI"
	rosAPI.delay = 50 * time.Millisecond
	log := makeTestLogger()
	subscriptionStartCount := 0
	startSubscription := func(ctx goContext.Context, pubURI string, log logging.Log) {
		subscriptionStartCount++
	}

	go sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
	defer sub.Shutdown()

	pubSend1 := []string{"pub1", "pub2"}
	pubSend2 := []string{"pub3"}

	// Send publishers.
	sub.pubListChan <- pubSend1

	// Override publishers
	sub.pubListChan <- pubSend2

	// Wait for published channels to take effect.
	<-time.After(60 * time.Millisecond)

	if reflect.DeepEqual(sub.pubList, pubSend2) == false {
		t.Fatalf("expected pubList to match sent publishers, got %v", sub.pubList)
	}

	if subscriptionStartCount != 1 {
		t.Fatalf("started %d subscriptions, expected 1", subscriptionStartCount)
	}
}

func TestSetDifference(t *testing.T) {
	testSetDifference := func(lhs []string, rhs []string, expected []string) {
		if result := setDifference(lhs, rhs); reflect.DeepEqual(expected, result) == false {
			t.Fatalf("setDifference(%v, %v) failed, expected %v, got %v", lhs, rhs, expected, result)
		}
	}

	testSetDifference([]string{"a", "b", "c"}, []string{"a", "b", "c"}, []string{})
	testSetDifference([]string{"a", "b", "c"}, []string{"a", "b"}, []string{"c"})
	testSetDifference([]string{"a", "b", "c"}, []string{"c", "b"}, []string{"a"})
	testSetDifference([]string{}, []string{"a", "b", "c"}, []string{})
	testSetDifference([]string{"a", "b", "c"}, []string{}, []string{"a", "b", "c"})
	testSetDifference([]string{"I", "am", "Sam"}, []string{"Sam", "I", "am"}, []string{})
	testSetDifference([]string{"She", "sells", "sea"}, []string{"shells", "on", "the", "sea", "shore"}, []string{"She", "sells"})
	testSetDifference([]string{"a", "a", "b", "b"}, []string{"b"}, []string{"a"})
	testSetDifference([]string{"a", "a", "b", "b"}, []string{"a"}, []string{"b"})
	testSetDifference([]string{"a", "b", "b"}, []string{"a", "a"}, []string{"b"})
}

// Test Helpers.

// makeTestSubscriber creates a subscriber with a simple u8[8] payload message type.
func makeTestSubscriber() *defaultSubscriber {
	return makeTestSubscriberWithJobCallback(func() {})
}

// makeTestSubscriberWithJobCallback creates a subscriber with a simple u8[8] payload message type and a job callback.
func makeTestSubscriberWithJobCallback(callback interface{}) *defaultSubscriber {
	fields := []gengo.Field{
		*gengo.NewField("Testing", "uint8", "u8", true, 8),
	}
	msgType := &DynamicMessageType{
		spec:         generateTestSpec(fields), // From dynamic_message_tests.go.
		nested:       make(map[string]*DynamicMessageType),
		jsonPrealloc: 0,
	}
	return newDefaultSubscriber("testTopic", msgType, callback)
}

// makeTestLogger creates a module logger for testing.
func makeTestLogger() logging.Log {
	log := logging.Root().With().Logger().Level(logging.ErrorLevel)
	return log
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

	topic := "/test/topic"
	nodeID := "testNode"
	msgChan := make(chan messageEvent)
	disconnectedChan := make(chan string)
	msgType := testMessageType{}
	log := makeTestLogger()

	startRemotePublisherConn(ctx, testDialer, pubURI, topic, msgType, nodeID, msgChan, disconnectedChan, log)

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
