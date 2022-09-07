package ros

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/team-rocos/rosgo/xmlrpc"
)

const (
	//APIStatusError is an API call which returned an Error
	APIStatusError = -1
	//APIStatusFailure is a failed API call
	APIStatusFailure = 0
	//APIStatusSuccess is a successful API call
	APIStatusSuccess = 1
	//Remap string constant for splitting components
	Remap = ":="
	//Default setting for OS interrupts
	defaultInterrupts = true
)

const defaultMasterAPITimeout time.Duration = 1 * time.Second

func processArguments(args []string) (NameMap, NameMap, NameMap, []string) {
	mapping := make(NameMap)
	params := make(NameMap)
	specials := make(NameMap)
	rest := make([]string, 0)
	for _, arg := range args {
		components := strings.Split(arg, Remap)
		if len(components) == 2 {
			key := components[0]
			value := components[1]
			if strings.HasPrefix(key, "__") {
				specials[key] = value
			} else if strings.HasPrefix(key, "_") {
				params[key[1:]] = value
			} else {
				mapping[key] = value
			}
		} else {
			rest = append(rest, arg)
		}
	}
	return mapping, params, specials, rest
}

// *defaultNode implements Node interface
// a defaultNode instance must be accessed in user goroutine.
type defaultNode struct {
	name             string
	namespace        string
	qualifiedName    string
	masterURI        string
	xmlrpcURI        string
	xmlrpcListener   net.Listener
	xmlrpcHandler    *xmlrpc.Handler
	xmlClient        *xmlrpc.XMLClient
	subscribers      map[string]*defaultSubscriber
	subscribersMutex sync.RWMutex
	publishers       map[string]*defaultPublisher
	publishersMutex  sync.RWMutex
	servers          map[string]*defaultServiceServer
	serversMutex     sync.RWMutex
	jobChan          chan func()
	interruptChan    chan os.Signal
	enableInterrupts bool
	log              zerolog.Logger
	ok               bool
	okMutex          sync.RWMutex
	waitGroup        sync.WaitGroup
	logDir           string
	hostname         string
	listenIP         string
	homeDir          string
	nameResolver     *NameResolver
	nonRosArgs       []string
}

// ServiceHeader is the header returned from probing a ros service, containing all type information
type ServiceHeader struct {
	Callerid     string
	Md5sum       string
	RequestType  string
	ResponseType string
	ServiceType  string
}

func listenRandomPort(address string, trialLimit int) (net.Listener, error) {
	var listener net.Listener
	var err error
	numTrial := 0
	for numTrial < trialLimit {
		port := 1024 + rand.Intn(65535-1024)
		addr := fmt.Sprintf("%s:%d", address, port)
		listener, err = net.Listen("tcp", addr)
		if err == nil {
			return listener, nil
		}
		numTrial++

	}
	return nil, fmt.Errorf("listenRandomPort exceeds trial limit")
}

func newDefaultNodeWithLogs(name string, log zerolog.Logger, args []string) (*defaultNode, error) {
	node, err := newDefaultNode(name, args)
	if err != nil {
		log.Error().Err(err).Msg("could not instantiate newDefaultNode")
		return nil, err
	}

	node.log = log
	return node, nil
}

func newDefaultNode(name string, args []string) (*defaultNode, error) {
	node := new(defaultNode)

	remapping, params, specials, rest := processArguments(args)

	node.homeDir = filepath.Join(os.Getenv("HOME"), ".ros")
	if homeDir := os.Getenv("ROS_HOME"); len(homeDir) > 0 {
		node.homeDir = homeDir
	}

	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.FatalLevel)
	if value, ok := specials["__ll"]; ok {
		val, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			log = log.Level(zerolog.Level(val))
		}
	}
	node.log = log

	// Parse the name, since if it's an absolute name, then technically it actually contains the namespace also.
	rawName := name
	if value, ok := specials["__name"]; ok {
		rawName = value
	}
	var namespace string
	var err error
	namespace, node.name, err = qualifyNodeName(rawName)
	if err != nil {
		return nil, err
	}

	node.namespace = namespace
	if ns := os.Getenv("ROS_NAMESPACE"); len(ns) > 0 {
		// Namespaces should all be absolute, so make sure it starts with a slash.
		node.namespace = GlobalNS + strings.TrimPrefix(ns, GlobalNS)
	}
	if ns, ok := specials["__ns"]; ok {
		// Namespaces should all be absolute, so make sure it starts with a slash.
		node.namespace = GlobalNS + strings.TrimPrefix(ns, GlobalNS)
	}
	if value, ok := specials["__si"]; ok {
		val, err := strconv.ParseBool(value)
		if err != nil {
			node.enableInterrupts = val
		}
	} else {
		node.enableInterrupts = defaultInterrupts
	}
	node.logDir = filepath.Join(node.homeDir, "log")
	if logDir := os.Getenv("ROS_LOG_DIR"); len(logDir) > 0 {
		node.logDir = logDir
	}
	if value, ok := specials["__log"]; ok {
		node.logDir = value
	}

	var onlyLocalhost bool
	node.hostname, onlyLocalhost = determineHost()
	if value, ok := specials["__hostname"]; ok {
		node.hostname = value
		onlyLocalhost = (value == "localhost")
	} else if value, ok := specials["__ip"]; ok {
		node.hostname = value
		onlyLocalhost = (value == "::1" || strings.HasPrefix(value, "127."))
	}
	if onlyLocalhost {
		node.listenIP = "127.0.0.1"
	} else {
		node.listenIP = "0.0.0.0"
	}

	node.masterURI = os.Getenv("ROS_MASTER_URI")
	if value, ok := specials["__master"]; ok {
		node.masterURI = value
	}

	node.nameResolver = newNameResolver(node.namespace, node.name, remapping)
	node.nonRosArgs = rest

	if node.namespace != GlobalNS {
		node.qualifiedName = node.namespace + Sep + node.name
	} else {
		node.qualifiedName = node.namespace + node.name
	}
	node.subscribers = make(map[string]*defaultSubscriber)
	node.publishers = make(map[string]*defaultPublisher)
	node.servers = make(map[string]*defaultServiceServer)
	node.interruptChan = make(chan os.Signal)
	node.ok = true

	// Install signal handler
	if node.enableInterrupts {
		signal.Notify(node.interruptChan, os.Interrupt)
		go func() {
			<-node.interruptChan
			log.Info().Msg("interrupted")
			node.okMutex.Lock()
			node.ok = false
			node.okMutex.Unlock()
		}()
	}
	node.jobChan = make(chan func())

	log.Debug().Str("master-uri", node.masterURI).Msg("")

	node.xmlClient = xmlrpc.NewXMLClient()
	node.xmlClient.Timeout = defaultMasterAPITimeout
	if st, ok := specials["__masterapitimeout"]; ok {
		if t, err := time.ParseDuration(st); err == nil {
			node.xmlClient.Timeout = t
		}
	}

	// Set parameters set by arguments
	for k, v := range params {
		_, err := callRosAPI(node.xmlClient, node.masterURI, "setParam", node.qualifiedName, k, v)
		if err != nil {
			return nil, err
		}
	}

	listener, err := listenRandomPort(node.listenIP, 10)
	if err != nil {
		log.Error().Err(err).Msg("")
		return nil, err
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		// Not reached
		log.Error().Err(err).Msg("")
		return nil, err
	}
	node.xmlrpcURI = fmt.Sprintf("http://%s:%s", node.hostname, port)
	log.Debug().Str("address", listener.Addr().String()).Msg("listening on http address")
	node.xmlrpcListener = listener
	m := map[string]xmlrpc.Method{
		"getBusStats":      func(callerID string) (interface{}, error) { return node.getBusStats(callerID) },
		"getBusInfo":       func(callerID string) (interface{}, error) { return node.getBusInfo(callerID) },
		"getMasterUri":     func(callerID string) (interface{}, error) { return node.getMasterURI(callerID) },
		"shutdown":         func(callerID string, msg string) (interface{}, error) { return node.shutdown(callerID, msg) },
		"getPid":           func(callerID string) (interface{}, error) { return node.getPid(callerID) },
		"getSubscriptions": func(callerID string) (interface{}, error) { return node.getSubscriptions(callerID) },
		"getPublications":  func(callerID string) (interface{}, error) { return node.getPublications(callerID) },
		"paramUpdate": func(callerID string, key string, value interface{}) (interface{}, error) {
			return node.paramUpdate(callerID, key, value)
		},
		"publisherUpdate": func(callerID string, topic string, publishers []interface{}) (interface{}, error) {
			return node.publisherUpdate(callerID, topic, publishers)
		},
		"requestTopic": func(callerID string, topic string, protocols []interface{}) (interface{}, error) {
			return node.requestTopic(callerID, topic, protocols)
		},
	}
	node.xmlrpcHandler = xmlrpc.NewHandler(m)
	go http.Serve(node.xmlrpcListener, node.xmlrpcHandler)
	log.Debug().Str("name", node.qualifiedName).Msg("started")
	return node, nil
}

func (node *defaultNode) OK() bool {
	node.okMutex.RLock()
	ok := node.ok
	node.okMutex.RUnlock()
	return ok
}

func (node *defaultNode) RemovePublisher(topic string) {
	node.publishersMutex.RLock()
	defer node.publishersMutex.RUnlock()

	name := node.nameResolver.remap(topic)

	if pub, ok := node.publishers[name]; ok {
		pub.Shutdown()
		delete(node.publishers, name)
	}

}

func (node *defaultNode) Name() string {
	return node.name
}

func (node *defaultNode) Namespace() string {
	return node.namespace
}

func (node *defaultNode) QualifiedName() string {
	return node.qualifiedName
}

func (node *defaultNode) getBusStats(callerID string) (interface{}, error) {
	return buildRosAPIResult(-1, "Not implemented", 0), nil
}

func (node *defaultNode) getBusInfo(callerID string) (interface{}, error) {
	return buildRosAPIResult(-1, "Not implemented", 0), nil
}

func (node *defaultNode) getMasterURI(callerID string) (interface{}, error) {
	return buildRosAPIResult(0, "Success", node.masterURI), nil
}

func (node *defaultNode) shutdown(callerID string, msg string) (interface{}, error) {
	node.okMutex.Lock()
	node.ok = false
	node.okMutex.Unlock()
	return buildRosAPIResult(APIStatusSuccess, "Success", 0), nil
}

func (node *defaultNode) getPid(callerID string) (interface{}, error) {
	return buildRosAPIResult(APIStatusSuccess, "Success", os.Getpid()), nil
}

func (node *defaultNode) getSubscriptions(callerID string) (interface{}, error) {
	node.subscribersMutex.RLock()
	defer node.subscribersMutex.RUnlock()

	result := []interface{}{}
	for t, s := range node.subscribers {
		pair := []interface{}{t, s.msgType.Name()}
		result = append(result, pair)
	}
	return buildRosAPIResult(APIStatusSuccess, "Success", result), nil
}

func (node *defaultNode) getPublications(callerID string) (interface{}, error) {
	node.publishersMutex.RLock()
	defer node.publishersMutex.RUnlock()

	result := []interface{}{}
	for topic, typ := range node.publishers {
		pair := []interface{}{topic, typ}
		result = append(result, pair)
	}
	return buildRosAPIResult(APIStatusSuccess, "Success", result), nil
}

func (node *defaultNode) paramUpdate(callerID string, key string, value interface{}) (interface{}, error) {
	return buildRosAPIResult(APIStatusError, "Not implemented", 0), nil
}

func (node *defaultNode) publisherUpdate(callerID string, topic string, publishers []interface{}) (interface{}, error) {
	node.log.Debug().Msg("slave API publisherUpdate() called")
	var code int32
	var message string
	if sub, ok := node.subscribers[topic]; !ok {
		node.log.Debug().Msg("publisherUpdate() called without subscribing topic")
		code = APIStatusFailure
		message = "No such topic"
	} else {
		pubURIs := make([]string, len(publishers))
		for i, URI := range publishers {
			pubURIs[i] = URI.(string)
		}
		sub.pubListChan <- pubURIs
		code = APIStatusSuccess
		message = "Success"
	}
	return buildRosAPIResult(code, message, 0), nil
}

func (node *defaultNode) requestTopic(callerID string, topic string, protocols []interface{}) (interface{}, error) {
	node.log.Debug().Str("callerID", callerID).Str("topic", topic).Msg("slave API requestTopic called")
	node.publishersMutex.RLock()
	defer node.publishersMutex.RUnlock()

	pub, ok := node.publishers[topic]
	if !ok {
		node.log.Debug().Msg("requestTopic() called with not publishing topic")
		return buildRosAPIResult(APIStatusFailure, "No such topic", nil), nil
	}

	selectedProtocol := make([]interface{}, 0)
	for _, v := range protocols {
		protocolParams := v.([]interface{})
		protocolName := protocolParams[0].(string)
		if protocolName == "TCPROS" {
			node.log.Debug().Msg("TCPROS requested")
			selectedProtocol = append(selectedProtocol, "TCPROS")
			host, portStr, err := pub.hostAndPort()
			if err != nil {
				return nil, err
			}
			p, err := strconv.ParseInt(portStr, 10, 32)
			if err != nil {
				return nil, err
			}
			port := int(p)
			selectedProtocol = append(selectedProtocol, host)
			selectedProtocol = append(selectedProtocol, port)
			break
		}
	}

	node.log.Debug().Interface("protocols", selectedProtocol).Msg("")
	return buildRosAPIResult(APIStatusSuccess, "Success", selectedProtocol), nil
}

func (node *defaultNode) NewPublisher(topic string, msgType MessageType) (Publisher, error) {
	name := node.nameResolver.remap(topic)
	return node.NewPublisherWithCallbacks(name, msgType, nil, nil)
}

func (node *defaultNode) NewPublisherWithCallbacks(topic string, msgType MessageType, connectCallback, disconnectCallback func(SingleSubscriberPublisher)) (Publisher, error) {
	node.publishersMutex.Lock()
	defer node.publishersMutex.Unlock()

	name := node.nameResolver.remap(topic)
	pub, ok := node.publishers[topic]
	if !ok {
		_, err := callRosAPI(node.xmlClient, node.masterURI, "registerPublisher",
			node.qualifiedName,
			name, msgType.Name(),
			node.xmlrpcURI)
		if err != nil {
			node.log.Error().Err(err).Msg("failed to call registerPublisher()")
			return nil, err
		}

		pub = newDefaultPublisher(node, name, msgType, connectCallback, disconnectCallback)
		node.publishers[name] = pub
		go pub.start(&node.waitGroup)
	}
	return pub, nil
}

// Master API for getSystemState
func (node *defaultNode) GetSystemState() ([]interface{}, error) {
	node.log.Trace().Msg("call Master API getSystemState")
	result, err := callRosAPI(node.xmlClient, node.masterURI, "getSystemState",
		node.qualifiedName)
	if err != nil {
		node.log.Error().Err(err).Msg("failed to call getSystemState()")
		return nil, err
	}
	list, ok := result.([]interface{})
	if !ok {
		node.log.Error().Str("type", reflect.TypeOf(result).String()).Msg("result is not []string")
	}
	node.log.Trace().Interface("result", list).Msg("")
	return list, nil
}

// Get Service string list via getSystemState
func (node *defaultNode) GetServiceList() ([]string, error) {
	// Get the system state
	sysState, err := node.GetSystemState()
	if err != nil {
		node.log.Error().Err(err).Msg("failed to call getSystemState()")
		return nil, err
	}
	serviceList := make([]string, 0)
	// Get the service list
	services := sysState[2].([]interface{})
	for _, s := range services {
		serviceItem := s.([]interface{})

		serviceList = append(serviceList, serviceItem[0].(string))
	}
	return serviceList, nil
}

// GetServiceType probes a service to return service type
func (node *defaultNode) GetServiceType(serviceName string) (*ServiceHeader, error) {

	// Result relative name
	serviceName = node.nameResolver.remap(serviceName)

	// Probe the service
	result, err := callRosAPI(node.xmlClient, node.masterURI, "lookupService", node.qualifiedName, serviceName)
	if err != nil {
		return nil, errors.Errorf("failed to lookup service %s : %s", serviceName, err)
	}

	serviceRawURL, converted := result.(string)
	if !converted {
		return nil, errors.Errorf("Result of 'lookupService' is not a string")
	}
	var serviceURL *url.URL
	if serviceURL, err = url.Parse(serviceRawURL); err != nil {
		return nil, err
	}
	var conn net.Conn
	if conn, err = net.Dial("tcp", serviceURL.Host); err != nil {
		return nil, err
	}

	// Write connection header
	var headers []header
	headers = append(headers, header{"probe", "1"})
	headers = append(headers, header{"md5sum", "*"})
	headers = append(headers, header{"callerid", node.qualifiedName})
	headers = append(headers, header{"service", serviceName})

	conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
	if err := writeConnectionHeader(headers, conn); err != nil {
		return nil, err
	}
	// Read response header
	conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
	resHeaders, err := readConnectionHeader(conn)
	if err != nil {
		return nil, err
	}
	// Convert headers to map
	resHeaderMap := make(map[string]string)
	for _, h := range resHeaders {
		resHeaderMap[h.key] = h.value
	}
	// Check whether the response was a successful header
	if len(resHeaders) == 1 {
		return nil, errors.Errorf("error probing service type: %s", resHeaders[0])
	}
	srvHeader := ServiceHeader{
		Callerid:     resHeaderMap["callerid"],
		Md5sum:       resHeaderMap["md5sum"],
		RequestType:  resHeaderMap["request_type"],
		ResponseType: resHeaderMap["response_type"],
		ServiceType:  resHeaderMap["type"],
	}
	return &srvHeader, nil
}

// Master API call for getPublishedTopics
func (node *defaultNode) GetPublishedTopics(subgraph string) (map[string]string, error) {
	node.log.Trace().Msg("call Master API getPublishedTopics")
	result, err := callRosAPI(node.xmlClient, node.masterURI, "getPublishedTopics",
		node.qualifiedName,
		subgraph)
	if err != nil {
		node.log.Error().Err(err).Msg("failed to call getPublishedTopics()")
		return nil, err
	}
	list, ok := result.([]interface{})
	if !ok {
		node.log.Error().Str("type", reflect.TypeOf(result).String()).Msg("result is not []string")
	}
	node.log.Trace().Interface("result", list).Msg("")
	// Convert to map
	topicMap := make(map[string]string)
	for _, v := range list {
		topic := v.([]interface{})
		topicMap[topic[0].(string)] = topic[1].(string)
	}
	return topicMap, nil
}

// GetPublishedActions uses PublishedTopics to find a topic that meets the action server requirements
func (node *defaultNode) GetPublishedActions(subgraph string) (map[string]string, error) {
	topics, err := node.GetPublishedTopics(subgraph)
	if err != nil {
		return nil, err
	}
	actionMap := make(map[string]string)
	for topicName, topicType := range topics {
		// if the published topic contains /result, it could be an action
		if strings.Contains(topicName, "/result") {

			prefix := strings.Replace(topicName, "/result", "", -1)
			// ensure its counterparts exist
			if (topics[prefix+"/status"] != "") && (topics[prefix+"/feedback"] != "") {
				// append action server uri to map including trimmed action type name
				actionMap[prefix] = strings.Replace(topicType, "ActionResult", "", -1)
			}
		}
	}
	return actionMap, nil
}

// Master API call for getTopicTypes
func (node *defaultNode) GetTopicTypes() []interface{} {
	node.log.Trace().Msg("call Master API getTopicTypes")
	result, err := callRosAPI(node.xmlClient, node.masterURI, "getTopicTypes",
		node.qualifiedName)
	if err != nil {
		node.log.Error().Err(err).Msg("failed to call getTopicTypes()")
	}
	list, ok := result.([]interface{})
	if !ok {
		node.log.Error().Str("type", reflect.TypeOf(result).String()).Msg("result is not []string")
	}
	node.log.Debug().Interface("result", list).Msg("")
	return list
}

// RemoveSubscriber shuts down and deletes an existing topic subscriber.
func (node *defaultNode) RemoveSubscriber(topic string) {
	name := node.nameResolver.remap(topic)
	if sub, ok := node.subscribers[name]; ok {
		sub.Shutdown()
		delete(node.subscribers, name)
	}
}

func (node *defaultNode) NewSubscriber(topic string, msgType MessageType, callback interface{}) (Subscriber, error) {
	// Respect the legacy interface
	return node.NewSubscriberWithFlowControl(topic, msgType, nil, callback)
}

func (node *defaultNode) NewSubscriberWithFlowControl(topic string, msgType MessageType, enableChan chan bool, callback interface{}) (Subscriber, error) {
	node.subscribersMutex.Lock()
	defer node.subscribersMutex.Unlock()

	name := node.nameResolver.remap(topic)
	sub, ok := node.subscribers[name]
	if !ok {
		node.log.Debug().Msg("call Master API registerSubscriber")
		result, err := callRosAPI(node.xmlClient, node.masterURI, "registerSubscriber",
			node.qualifiedName,
			name,
			msgType.Name(),
			node.xmlrpcURI)
		if err != nil {
			node.log.Error().Err(err).Msg("failed to call registerSubscriber()")
			return nil, err
		}
		list, ok := result.([]interface{})
		if !ok {
			node.log.Error().Str("type", reflect.TypeOf(result).String()).Msg("result is not []string")
		}
		var publishers []string
		for _, item := range list {
			s, ok := item.(string)
			if !ok {
				node.log.Error().Msg("publisher list contains no string object")
			}
			publishers = append(publishers, s)
		}

		node.log.Debug().Strs("publishers", publishers).Msg("")

		sub = newDefaultSubscriber(name, msgType, callback)
		node.subscribers[name] = sub

		node.log.Debug().Str("topic", sub.topic).Msg("start subscriber goroutine for topic")
		go sub.start(&node.waitGroup, node.qualifiedName, node.xmlrpcURI, node.masterURI, node.jobChan, enableChan, node.log)
		node.log.Debug().Msg("done")
		sub.pubListChan <- publishers
		node.log.Debug().Str("topic", sub.topic).Msg("update publisher list for topic")
	} else {
		sub.callbacks = append(sub.callbacks, callback)
	}
	return sub, nil
}

func (node *defaultNode) NewServiceClient(service string, srvType ServiceType) ServiceClient {
	name := node.nameResolver.remap(service)
	client := newDefaultServiceClient(node.log, node.qualifiedName, node.masterURI, name, srvType)
	return client
}

func (node *defaultNode) NewServiceServer(service string, srvType ServiceType, handler interface{}) ServiceServer {
	node.serversMutex.Lock()
	defer node.serversMutex.Unlock()

	name := node.nameResolver.remap(service)
	server, ok := node.servers[name]
	if ok {
		server.Shutdown()
	}

	server = newDefaultServiceServer(node, name, srvType, handler)
	if server == nil {
		return nil
	}

	node.servers[name] = server
	return server
}

func (node *defaultNode) SpinOnce() bool {
	timeoutChan := time.After(10 * time.Millisecond)
	select {
	case job := <-node.jobChan:
		job()
		return false
	case <-timeoutChan:
		break
	}
	return true
}

func (node *defaultNode) Spin() {
	for node.OK() {
		timeoutChan := time.After(1000 * time.Millisecond)
		select {
		case job := <-node.jobChan:
			node.log.Debug().Msg("execute job")
			job()
		case <-timeoutChan:
		}
	}
}

func (node *defaultNode) Shutdown() {
	node.log.Debug().Msg("shutting node down")
	node.okMutex.Lock()
	node.ok = false
	node.okMutex.Unlock()
	node.log.Debug().Msg("shutdown subscribers")
	for _, s := range node.subscribers {
		s.Shutdown()
	}
	node.log.Debug().Msg("shutdown subscribers...done")
	node.log.Debug().Msg("shutdown publishers")
	for _, p := range node.publishers {
		p.Shutdown()
	}
	node.log.Debug().Msg("shutdown publishers...done")
	node.log.Debug().Msg("shutdown servers")
	for _, s := range node.servers {
		s.Shutdown()
	}
	node.log.Debug().Msg("shutdown servers...done")
	node.log.Debug().Msg("wait all goroutines")
	node.waitGroup.Wait()
	node.log.Debug().Msg("wait all goroutines...done")
	node.log.Debug().Msg("close XMLRPC listener")
	node.xmlrpcListener.Close()
	node.log.Debug().Msg("close XMLRPC done")
	node.log.Debug().Msg("wait XMLRPC server shutdown")
	node.xmlrpcHandler.WaitForShutdown()
	node.log.Debug().Msg("wait XMLRPC server shutdown...done")
	node.log.Debug().Msg("shutting node down completed")
}

func (node *defaultNode) GetParamNames() ([]string, error) {
	node.log.Trace().Msg("call Master API getParamNames")
	result, err := callRosAPI(node.xmlClient, node.masterURI, "getParamNames", node.qualifiedName)
	if err != nil {
		node.log.Error().Err(err).Msg("failed to call getParamNames()")
		return nil, err
	}
	list, ok := result.([]string)
	if !ok {
		node.log.Error().Str("type", reflect.TypeOf(result).String()).Msg("result is not []string")
	}
	node.log.Trace().Interface("result", list).Msg("response from getParamNames()")
	return list, nil
}

func (node *defaultNode) GetParam(key string) (interface{}, error) {
	name := node.nameResolver.remap(key)
	return callRosAPI(node.xmlClient, node.masterURI, "getParam", node.qualifiedName, name)
}

func (node *defaultNode) SetParam(key string, value interface{}) error {
	name := node.nameResolver.remap(key)
	_, e := callRosAPI(node.xmlClient, node.masterURI, "setParam", node.qualifiedName, name, value)
	return e
}

func (node *defaultNode) HasParam(key string) (bool, error) {
	name := node.nameResolver.remap(key)
	result, err := callRosAPI(node.xmlClient, node.masterURI, "hasParam", node.qualifiedName, name)
	if err != nil {
		return false, err
	}
	hasParam := result.(bool)
	return hasParam, nil
}

func (node *defaultNode) SearchParam(key string) (string, error) {
	result, err := callRosAPI(node.xmlClient, node.masterURI, "searchParam", node.qualifiedName, key)
	if err != nil {
		return "", err
	}
	foundKey := result.(string)
	return foundKey, nil
}

func (node *defaultNode) DeleteParam(key string) error {
	name := node.nameResolver.remap(key)
	_, err := callRosAPI(node.xmlClient, node.masterURI, "deleteParam", node.qualifiedName, name)
	return err
}

func (node *defaultNode) Logger() zerolog.Logger {
	return node.log
}

func (node *defaultNode) NonRosArgs() []string {
	return node.nonRosArgs
}

func loadParamFromString(s string) (interface{}, error) {
	decoder := json.NewDecoder(strings.NewReader(s))
	var value interface{}
	err := decoder.Decode(&value)
	if err != nil {
		return nil, err
	}
	return value, err
}
