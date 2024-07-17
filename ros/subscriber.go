package ros

import (
	"bytes"
	goContext "context"
	"fmt"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/dronedeploy/rosgo/xmlrpc"
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
	xmlClient  *xmlrpc.XMLClient
}

// RequestTopicURI requests the URI of a given topic from a publisher.
func (a *SubscriberRosAPI) RequestTopicURI(pub string) (string, error) {
	protocols := []interface{}{[]interface{}{"TCPROS"}}
	result, err := callRosAPI(a.xmlClient, pub, "requestTopic", a.nodeID, a.topic, protocols)

	if err != nil {
		return "", err
	}

	protocolParams := result.([]interface{})

	if n := len(protocolParams); n < 3 {
		return "", errors.New("invalid requestTopic result with length " + fmt.Sprint(n))
	}

	if name := protocolParams[0].(string); name != "TCPROS" {
		return "", errors.New("rosgo does not support protocol: " + name)
	}

	addr, ok := protocolParams[1].(string)
	if !ok {
		return "", errors.New("failed to extract addr from requestTopic result")
	}

	port, ok := protocolParams[2].(int32)
	if !ok {
		return "", errors.New("failed to extract port from requestTopic result")
	}

	uri := fmt.Sprintf("%s:%d", addr, port)
	return uri, nil
}

// Unregister removes a subscriber from a topic.
func (a *SubscriberRosAPI) Unregister() error {
	_, err := callRosAPI(a.xmlClient, a.masterURI, "unregisterSubscriber", a.nodeID, a.topic, a.nodeAPIURI)
	return err
}

var _ SubscriberRos = &SubscriberRosAPI{}

// requestTopicResult represents the important data returned from a requestTopic call.
type requestTopicResult struct {
	pub string
	uri string
	err error
}

// startPublisherSubscription defines a function interface for starting a subscription.
type startPublisherSubscription func(ctx goContext.Context, pubURI string, log zerolog.Logger)

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

func (sub *defaultSubscriber) start(wg *sync.WaitGroup, nodeID string, nodeAPIURI string, masterURI string, jobChan chan func(), enableChan chan bool, log zerolog.Logger) {
	ctx, cancel := goContext.WithCancel(goContext.Background())
	defer cancel()

	log.Debug().Str("topic", sub.topic).Msg("subscriber goroutine for topic started")

	wg.Add(1)
	defer wg.Done()
	defer func() {
		log.Debug().Str("topic", sub.topic).Msg("defaultSubscriber.start exit")
	}()

	// Construct the SubscriberRosApi.
	rosAPI := &SubscriberRosAPI{
		topic:      sub.topic,
		nodeID:     nodeID,
		masterURI:  masterURI,
		nodeAPIURI: nodeAPIURI,
	}
	rosAPI.xmlClient = xmlrpc.NewXMLClient()
	rosAPI.xmlClient.Timeout = defaultMasterAPITimeout

	// Decouples the implementation details of starting a subscription from the run loop.
	startSubscription := func(ctx goContext.Context, pubURI string, log zerolog.Logger) {
		startRemotePublisherConn(ctx, &TCPRosNetDialer{}, pubURI, sub.topic, sub.msgType, nodeID, sub.msgChan, sub.disconnectedChan, log)
	}

	// Setup is complete, run the subscriber.
	sub.run(ctx, jobChan, enableChan, rosAPI, startSubscription, log)
}

func (sub *defaultSubscriber) run(ctx goContext.Context, jobChan chan func(), enableChan chan bool, rosAPI SubscriberRos, startSubscription startPublisherSubscription, log zerolog.Logger) {
	enabled := true
	cancelMap := make(map[string]goContext.CancelFunc)
	uri2pubMap := make(map[string]string)

	var activeJobChan chan func()
	var latestJob func()

	var requestTopicChan chan requestTopicResult
	var requestTopicCancel goContext.CancelFunc

	for {
		select {
		case list := <-sub.pubListChan:
			// Cancel any current fetches for new publishers.
			if requestTopicCancel != nil {
				requestTopicCancel()
			}
			log.Debug().Str("topic", sub.topic).Msg("receive pubListChan")
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

			// Make a new request topic channel - meaning pending old requests will get ignored.
			requestTopicChan = make(chan requestTopicResult)

			var requestTopicCtx goContext.Context
			requestTopicCtx, requestTopicCancel = goContext.WithCancel(ctx)
			defer requestTopicCancel()

			// Handle requesting URIs in a seperate go routine.
			go func(thisCtx goContext.Context, publishers []string, resultChan chan requestTopicResult) {
				for _, pub := range publishers {
					uri, err := rosAPI.RequestTopicURI(pub)

					// Attempt to update the main loop with new results.
					select {
					case <-thisCtx.Done():
						return
					case resultChan <- requestTopicResult{pub, uri, err}:
					}
				}
			}(requestTopicCtx, newPubs, requestTopicChan)

		case requestTopicData := <-requestTopicChan:
			pub := requestTopicData.pub
			uri := requestTopicData.uri

			if err := requestTopicData.err; err != nil {
				log.Error().Str("topic", sub.topic).Err(err).Msg("uri request failed for topic with error")
				continue
			}

			uri2pubMap[uri] = pub
			subCtx, cancel := goContext.WithCancel(ctx)
			defer cancel()
			sub.pubList = append(sub.pubList, pub)
			cancelMap[pub] = cancel
			startSubscription(subCtx, uri, log)

		case uri := <-sub.disconnectedChan:
			log.Debug().Str("uri", uri).Msg("connection to uri was disconnected")
			if pub, ok := uri2pubMap[uri]; ok {
				if cancel, ok := cancelMap[pub]; ok {
					cancel()
					delete(cancelMap, pub)
				}
				delete(uri2pubMap, uri)
				sub.pubList = setDifference(sub.pubList, []string{pub})
			}

		case callback := <-sub.addCallbackChan:
			log.Debug().Str("topic", sub.topic).Msg("receive addCallbackChan")
			sub.callbacks = append(sub.callbacks, callback)

		case msgEvent := <-sub.msgChan:
			if !enabled {
				continue
			}
			// Pop received message then bind callbacks and enqueue to the job channel.
			log.Trace().Str("topic", sub.topic).Msg("receive msgChan")

			callbacks := make([]interface{}, len(sub.callbacks))
			copy(callbacks, sub.callbacks)

			// Prepare the latest job to be passed on.
			latestJob = func() {
				m := sub.msgType.NewMessage()
				reader := bytes.NewReader(msgEvent.bytes)
				if err := m.Deserialize(reader); err != nil {
					log.Error().Str("topic", sub.topic).Err(err).Msg("")
					return
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
			log.Trace().Str("topic", sub.topic).Msg("callback job enqueued")
			activeJobChan = nil
			latestJob = func() {}

		case <-sub.shutdownChan:
			// Shutdown subscription goroutine; keeps shutdowns snappy.
			go func() {
				log.Debug().Str("topic", sub.topic).Msg("receive shutdownChan")
				if err := rosAPI.Unregister(); err != nil {
					log.Warn().Str("topic", sub.topic).Err(err).Msg("unregister error")
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
	log zerolog.Logger) {
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
	// Dedup - remove duplicatees.
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
