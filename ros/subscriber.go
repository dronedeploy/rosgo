package ros

import (
	"bytes"
	goContext "context"
	"fmt"
	"reflect"
	"sync"

	modular "github.com/edwinhayes/logrus-modular"
	"github.com/pkg/errors"
)

type messageEvent struct {
	bytes []byte
	event MessageEvent
}

type subscriptionChannels struct {
	enableMessages chan bool
}

// SubscriberRos interface provides methods to decouple ROS API calls from the subscriber itself.
type SubscriberRos interface {
	RequestTopicURI(pub string) (string, error)
	Unregister() error
}

// SubscriberRosAPI implements SubscriberRos using callRosAPI rpc calls.
type SubscriberRosAPI struct {
	topic      string
	nodeID     string
	nodeAPIURI string
	masterURI  string
}

// RequestTopicURI requests the URI of a given topic from a publisher.
func (a *SubscriberRosAPI) RequestTopicURI(pub string) (string, error) {
	protocols := []interface{}{[]interface{}{"TCPROS"}}
	result, err := callRosAPI(pub, "requestTopic", a.nodeID, a.topic, protocols)

	if err != nil {
		return "", err
	}

	protocolParams := result.([]interface{})

	if name := protocolParams[0].(string); name != "TCPROS" {
		return "", errors.New("rosgo does not support protocol: " + name)
	}

	addr := protocolParams[1].(string)
	port := protocolParams[2].(int32)
	uri := fmt.Sprintf("%s:%d", addr, port)
	return uri, nil
}

// Unregister removes a subscriber from a topic.
func (a *SubscriberRosAPI) Unregister() error {
	_, err := callRosAPI(a.masterURI, "unregisterSubscriber", a.nodeID, a.topic, a.nodeAPIURI)
	return err
}

var _ SubscriberRos = &SubscriberRosAPI{}

// requestTopicResult represents the important data returned from a requestTopic call.
type requestTopicResult struct {
	pub string
	uri string
	err error
}

// startPublosherSubscription defines a function interface for starting a subscription in run.
type startPublisherSubscription func(ctx goContext.Context, pubURI string, log *modular.ModuleLogger)

// The subscriber object runs in own goroutine (start).
type defaultSubscriber struct {
	topic            string
	msgType          MessageType
	pubList          []string
	pubListChan      chan []string
	msgChan          chan messageEvent
	callbacks        []interface{}
	addCallbackChan  chan interface{}
	shutdownChan     chan struct{}
	cancel           map[string]goContext.CancelFunc
	uri2pub          map[string]string
	disconnectedChan chan string
}

func newDefaultSubscriber(topic string, msgType MessageType, callback interface{}) *defaultSubscriber {
	sub := new(defaultSubscriber)
	sub.topic = topic
	sub.msgType = msgType
	sub.msgChan = make(chan messageEvent)
	sub.pubListChan = make(chan []string)
	sub.addCallbackChan = make(chan interface{})
	sub.shutdownChan = make(chan struct{})
	sub.disconnectedChan = make(chan string)
	sub.callbacks = []interface{}{callback}
	return sub
}

func (sub *defaultSubscriber) start(wg *sync.WaitGroup, nodeID string, nodeAPIURI string, masterURI string, jobChan chan func(), enableChan chan bool, log *modular.ModuleLogger) {
	ctx, cancel := goContext.WithCancel(goContext.Background())
	defer cancel()
	logger := *log
	logger.Debugf("Subscriber goroutine for %s started.", sub.topic)

	wg.Add(1)
	defer wg.Done()
	defer func() {
		logger.Debug(sub.topic, " : defaultSubscriber.start exit")
	}()

	// Construct the SubscriberRosApi.
	rosAPI := &SubscriberRosAPI{
		topic:      sub.topic,
		nodeID:     nodeID,
		masterURI:  masterURI,
		nodeAPIURI: nodeAPIURI,
	}

	// Decouples a bunch of implementation details from the actual run logic.
	startSubscription := func(ctx goContext.Context, pubURI string, log *modular.ModuleLogger) {
		startRemotePublisherConn(ctx, &TCPRosNetDialer{}, pubURI, sub.topic, sub.msgType, nodeID, sub.msgChan, sub.disconnectedChan, log)
	}

	// Setup is complete, run the subscriber.
	sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
}

func (sub *defaultSubscriber) run(ctx goContext.Context, jobChan chan func(), enableChan chan bool, rosAPI SubscriberRos, startSubscription startPublisherSubscription, log *modular.ModuleLogger) {
	logger := *log
	enabled := true
	cancelMap := make(map[string]goContext.CancelFunc)
	uri2pubMap := make(map[string]string)

	var activeJobChan chan func()
	var latestJob func()

	requestTopicChan := make(chan requestTopicResult)

	var newPubsCancel goContext.CancelFunc

	for {
		select {
		case list := <-sub.pubListChan:
			if newPubsCancel != nil {
				newPubsCancel()
			}
			logger.Debug(sub.topic, " : Receive pubListChan")
			deadPubs := setDifference(sub.pubList, list)
			newPubs := setDifference(list, sub.pubList)
			sub.pubList = setDifference(sub.pubList, deadPubs)
			for _, pub := range deadPubs {
				if cancel, ok := cancelMap[pub]; ok {
					cancel()
					delete(cancelMap, pub)
				}
				for uri, pubInMap := range uri2pubMap {
					if pub == pubInMap {
						delete(uri2pubMap, uri)
					}
				}
			}

			var newPubsCtx goContext.Context
			newPubsCtx, newPubsCancel = goContext.WithCancel(ctx)
			defer newPubsCancel()

			// Handle requesting URIs in a seperate go routine
			go func(thisCtx goContext.Context) {
				for _, pub := range newPubs {
					uri, err := rosAPI.RequestTopicURI(pub)

					// Force the context cancellation to take priority.
					select {
					case <-thisCtx.Done():
						return
					default:
					}

					select {
					case <-thisCtx.Done():
						return
					case requestTopicChan <- requestTopicResult{pub, uri, err}:
					}
				}
			}(newPubsCtx)

		case requestTopicData := <-requestTopicChan:
			pub := requestTopicData.pub
			uri := requestTopicData.uri

			if err := requestTopicData.err; err != nil {
				logger.Error("uri request failed, topic : ", sub.topic, ", error : ", err)
				continue
			}

			uri2pubMap[uri] = pub
			subCtx, cancel := goContext.WithCancel(ctx)
			defer cancel()
			sub.pubList = append(sub.pubList, pub)
			cancelMap[pub] = cancel
			startSubscription(subCtx, uri, log)

		case uri := <-sub.disconnectedChan:
			logger.Debugf("Connection to %s was disconnected.", uri)
			if pub, ok := uri2pubMap[uri]; ok {
				if cancel, ok := cancelMap[pub]; ok {
					cancel()
					delete(cancelMap, pub)
				}
				delete(uri2pubMap, uri)
				sub.pubList = setDifference(sub.pubList, []string{pub})
			}

		case callback := <-sub.addCallbackChan:
			logger.Debug(sub.topic, " : Receive addCallbackChan")
			sub.callbacks = append(sub.callbacks, callback)

		case msgEvent := <-sub.msgChan:
			if enabled == false {
				continue
			}
			// Pop received message then bind callbacks and enqueue to the job channel.
			logger.Debug(sub.topic, " : Receive msgChan")

			callbacks := make([]interface{}, len(sub.callbacks))
			copy(callbacks, sub.callbacks)

			// Prepare the latest job to be passed on.
			latestJob = func() {
				m := sub.msgType.NewMessage()
				reader := bytes.NewReader(msgEvent.bytes)
				if err := m.Deserialize(reader); err != nil {
					logger.Error(sub.topic, " : ", err)
				}
				// TODO: Investigate this
				args := []reflect.Value{reflect.ValueOf(m), reflect.ValueOf(msgEvent.event)}
				for _, callback := range callbacks {
					fun := reflect.ValueOf(callback)
					numArgsNeeded := fun.Type().NumIn()
					if numArgsNeeded <= 2 {
						fun.Call(args[:numArgsNeeded])
					}
				}
			}
			activeJobChan = jobChan

		case activeJobChan <- latestJob:
			logger.Debug(sub.topic, " : Callback job enqueued.")
			activeJobChan = nil
			latestJob = func() {}

		case <-sub.shutdownChan:
			// Shutdown subscription goroutine; keeps shutdowns snappy.
			go func() {
				logger.Debug(sub.topic, " : receive shutdownChan")
				if err := rosAPI.Unregister(); err != nil {
					logger.Warn(sub.topic, " : unregister error: ", err)
				}
			}()
			sub.shutdownChan <- struct{}{}
			return

		case enabled = <-enableChan:
			// Stop any active jobs trying to get in the queue.
			activeJobChan = nil
			latestJob = func() {}
		}
	}
}

// startRemotePublisherConn creates a subscription to a remote publisher and runs it.
func startRemotePublisherConn(ctx goContext.Context, dialer TCPRosDialer,
	pubURI string, topic string, msgType MessageType, nodeID string,
	msgChan chan messageEvent,
	disconnectedChan chan string,
	log *modular.ModuleLogger) {
	sub := newDefaultSubscription(pubURI, topic, msgType, nodeID, msgChan, disconnectedChan)
	sub.dialer = dialer
	sub.startWithContext(ctx, log)
}

// setDifference returns the difference of two "sets" represented by string arrays.
func setDifference(lhs []string, rhs []string) []string {
	result := make([]string, 0)
	for _, entry := range lhs {
		hasEntry := false
		for _, nEntry := range rhs {
			if entry == nEntry {
				hasEntry = true
				break
			}
		}
		if hasEntry == false {
			result = append(result, entry)
		}
	}
	// Dedup
	for i, entry := range result {
		for j := i + 1; j < len(result); j++ {
			if entry == result[j] {
				result = append(result[:j], result[j+1:]...)
			}
		}
	}
	return result
}

func (sub *defaultSubscriber) Shutdown() {
	sub.shutdownChan <- struct{}{}
	<-sub.shutdownChan
}

func (sub *defaultSubscriber) GetNumPublishers() int {
	return len(sub.pubList)
}
