package ros

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"time"

	modular "github.com/edwinhayes/logrus-modular"
)

type messageEvent struct {
	bytes []byte
	event MessageEvent
}

type subscriptionChannels struct {
	quit           chan struct{}
	enableMessages chan bool
}

// The subscriber object runs in own goroutine (start).
type defaultSubscriber struct {
	topic             string
	msgType           MessageType
	pubList           []string
	pubListChan       chan []string
	msgChan           chan messageEvent
	callbacks         []interface{}
	addCallbackChan   chan interface{}
	shutdownChan      chan struct{}
	subscriptionChans map[string]subscriptionChannels
	uri2pub           map[string]string
	disconnectedChan  chan string
}

func newDefaultSubscriber(topic string, msgType MessageType, callback interface{}) *defaultSubscriber {
	sub := new(defaultSubscriber)
	sub.topic = topic
	sub.msgType = msgType
	sub.msgChan = make(chan messageEvent, 10)
	sub.pubListChan = make(chan []string, 10)
	sub.addCallbackChan = make(chan interface{}, 10)
	sub.shutdownChan = make(chan struct{})
	sub.disconnectedChan = make(chan string, 10)
	sub.uri2pub = make(map[string]string)
	sub.subscriptionChans = make(map[string]subscriptionChannels)
	sub.callbacks = []interface{}{callback}
	return sub
}

func (sub *defaultSubscriber) start(wg *sync.WaitGroup, nodeID string, nodeAPIURI string, masterURI string, jobChan chan func(), enableChan chan bool, log *modular.ModuleLogger) {
	logger := *log
	logger.Debugf("Subscriber goroutine for %s started.", sub.topic)
	wg.Add(1)
	defer wg.Done()
	defer func() {
		logger.Debug(sub.topic, " : defaultSubscriber.start exit")
	}()
	for {
		select {
		case list := <-sub.pubListChan:
			logger.Debug(sub.topic, " : Receive pubListChan")
			deadPubs := setDifference(sub.pubList, list)
			newPubs := setDifference(list, sub.pubList)
			sub.pubList = list
			for _, pub := range deadPubs {
				delete(sub.subscriptionChans, pub)
			}

			for _, pub := range newPubs {
				protocols := []interface{}{[]interface{}{"TCPROS"}}
				result, err := callRosAPI(pub, "requestTopic", nodeID, sub.topic, protocols)
				if err != nil {
					logger.Error(sub.topic, " : ", err)
					continue
				}

				protocolParams := result.([]interface{})
				for _, x := range protocolParams {
					logger.Debug(sub.topic, " : ", x)
				}

				name := protocolParams[0].(string)
				if name == "TCPROS" {
					addr := protocolParams[1].(string)
					port := protocolParams[2].(int32)
					uri := fmt.Sprintf("%s:%d", addr, port)
					quitChan := make(chan struct{})
					enableMessagesChan := make(chan bool)
					sub.uri2pub[uri] = pub
					sub.subscriptionChans[pub] = subscriptionChannels{quit: quitChan, enableMessages: enableMessagesChan}
					startRemotePublisherConn(log, uri, sub.topic, sub.msgType, nodeID, sub.msgChan, enableMessagesChan, quitChan, sub.disconnectedChan)
				} else {
					logger.Warn(sub.topic, " : rosgo does not support protocol: ", name)
				}
			}

		case callback := <-sub.addCallbackChan:
			logger.Debug(sub.topic, " : Receive addCallbackChan")
			sub.callbacks = append(sub.callbacks, callback)

		case msgEvent := <-sub.msgChan:
			// Pop received message then bind callbacks and enqueue to the job channel.
			logger.Debug(sub.topic, " : Receive msgChan")

			callbacks := make([]interface{}, len(sub.callbacks))
			copy(callbacks, sub.callbacks)
			select {
			case jobChan <- func() {
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
						fun.Call(args[0:numArgsNeeded])
					}
				}
			}:
				logger.Debug(sub.topic, " : Callback job enqueued.")
			case <-time.After(time.Duration(3) * time.Second):
				logger.Debug(sub.topic, " : Callback job timed out.")
			}
			logger.Debug("Callback job enqueued.")

		case pubURI := <-sub.disconnectedChan:
			logger.Debugf("Connection to %s was disconnected.", pubURI)
			pub := sub.uri2pub[pubURI]
			delete(sub.subscriptionChans, pub)
			delete(sub.uri2pub, pubURI)

		case <-sub.shutdownChan:
			// Shutdown subscription goroutine.
			logger.Debug(sub.topic, " : Receive shutdownChan")
			for _, closeChan := range sub.subscriptionChans {
				close(closeChan.quit)
			}
			_, err := callRosAPI(masterURI, "unregisterSubscriber", nodeID, sub.topic, nodeAPIURI)
			if err != nil {
				logger.Warn(sub.topic, " : ", err)
			}
			sub.shutdownChan <- struct{}{}
			return

		case enabled := <-enableChan:
			for _, subscription := range sub.subscriptionChans {
				subscription.enableMessages <- enabled
			}
		}
	}
}

// startRemotePublisherConn creates a subscription to a remote publisher and runs it.
func startRemotePublisherConn(log *modular.ModuleLogger,
	pubURI string, topic string, msgType MessageType, nodeID string,
	msgChan chan messageEvent,
	enableMessagesChan chan bool,
	quitChan chan struct{},
	disconnectedChan chan string) {
	sub := newDefaultSubscription(pubURI, topic, msgType, nodeID, msgChan, enableMessagesChan, quitChan, disconnectedChan)
	sub.start(log)
}

func setDifference(lhs []string, rhs []string) []string {
	left := map[string]bool{}
	for _, item := range lhs {
		left[item] = true
	}
	right := map[string]bool{}
	for _, item := range rhs {
		right[item] = true
	}
	for k := range right {
		delete(left, k)
	}
	var result []string
	for k := range left {
		result = append(result, k)
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
