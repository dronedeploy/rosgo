package ros

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type remoteSubscriberSessionError struct {
	session *remoteSubscriberSession
	err     error
}

func (e *remoteSubscriberSessionError) Error() string {
	return fmt.Sprintf("remoteSubscriberSession: %s topic: %s error: %v",
		e.session.callerID, e.session.topic, e.err)
}

type defaultPublisher struct {
	node               *defaultNode
	topic              string
	msgType            MessageType
	msgChan            chan []byte
	shutdownChan       chan struct{}
	sesssionIDCount    int
	sessions           map[int]*remoteSubscriberSession
	sessionChan        chan *remoteSubscriberSession
	sessionErrorChan   chan error
	listenerErrorChan  chan error
	listener           net.Listener
	connectCallback    func(SingleSubscriberPublisher)
	disconnectCallback func(SingleSubscriberPublisher)
}

func newDefaultPublisher(node *defaultNode,
	topic string, msgType MessageType,
	connectCallback, disconnectCallback func(SingleSubscriberPublisher)) *defaultPublisher {
	pub := new(defaultPublisher)
	pub.node = node
	pub.topic = topic
	pub.msgType = msgType
	pub.shutdownChan = make(chan struct{}, 10)
	pub.sessions = make(map[int]*remoteSubscriberSession)
	pub.msgChan = make(chan []byte, 10)
	pub.listenerErrorChan = make(chan error, 10)
	pub.sessionChan = make(chan *remoteSubscriberSession, 10)
	pub.sessionErrorChan = make(chan error, 10)
	pub.connectCallback = connectCallback
	pub.disconnectCallback = disconnectCallback
	if listener, err := listenRandomPort(node.listenIP, 10); err != nil {
		panic(err)
	} else {
		pub.listener = listener
	}
	return pub
}

func (pub *defaultPublisher) start(wg *sync.WaitGroup) {
	log := pub.node.log
	log.Debug().Str("topic", pub.topic).Msg("publisher goroutine for topic started")
	wg.Add(1)
	defer func() {
		log.Debug().Msg("defaultPublisher.start exit")
		wg.Done()
	}()

	go pub.listenRemoteSubscriber()

	for {
		log.Debug().Msg("defaultPublisher.start loop")
		select {
		case msg := <-pub.msgChan:
			for _, s := range pub.sessions {
				session := s
				session.msgChan <- msg
			}

		case err := <-pub.listenerErrorChan:
			log.Debug().Err(err).Msg("listener closed unexpectedly")
			pub.listener.Close()
			return

		case s := <-pub.sessionChan:
			pub.sessions[s.id] = s
			go s.start()

		case err := <-pub.sessionErrorChan:
			log.Error().Err(err).Msg("")
			if sessionError, ok := err.(*remoteSubscriberSessionError); ok {
				id := sessionError.session.id
				delete(pub.sessions, id)
			}

		case <-pub.shutdownChan:
			log.Debug().Msg("defaultPublisher.start Receive shutdownChan")
			pub.listener.Close()
			log.Debug().Msg("defaultPublisher.start closed listener")
			_, err := callRosAPI(pub.node.xmlClient, pub.node.masterURI, "unregisterPublisher", pub.node.qualifiedName, pub.topic, pub.node.xmlrpcURI)
			if err != nil {
				log.Warn().Err(err).Msg("")
			}

			for id, s := range pub.sessions {
				s.quitChan <- struct{}{}
				delete(pub.sessions, id)
			}
			pub.shutdownChan <- struct{}{}
			return
		}
	}
}

func (pub *defaultPublisher) listenRemoteSubscriber() {
	log := pub.node.log
	log.Debug().Str("address", pub.listener.Addr().String()).Msg("start listening")
	defer func() {
		log.Debug().Msg("defaultPublisher.listenRemoteSubscriber exit")
	}()

	for {
		log.Debug().Msg("defaultPublisher.listenRemoteSubscriber loop")
		conn, err := pub.listener.Accept()
		if err != nil {
			log.Debug().Msg("pub.listner.Accept() failed")
			pub.listenerErrorChan <- err
			close(pub.listenerErrorChan)
			log.Debug().Msg("defaultPublisher.listenRemoteSubscriber loop exit")
			return
		}

		log.Debug().Str("address", conn.RemoteAddr().String()).Msg("connected")
		id := pub.sesssionIDCount
		pub.sesssionIDCount++
		session := newRemoteSubscriberSession(pub, id, conn)
		pub.sessionChan <- session
	}
}

func (pub *defaultPublisher) TryPublish(msg Message) error {
	var buf bytes.Buffer
	err := msg.Serialize(&buf)
	if err != nil {
		return errors.Wrap(err, "failed to serialize message:")
	}
	pub.msgChan <- buf.Bytes()
	return nil
}

func (pub *defaultPublisher) Publish(msg Message) {
	var buf bytes.Buffer
	_ = msg.Serialize(&buf)
	pub.msgChan <- buf.Bytes()
}

func (pub *defaultPublisher) GetNumSubscribers() int {
	return len(pub.sessions)
}

func (pub *defaultPublisher) Shutdown() {
	pub.shutdownChan <- struct{}{}
	<-pub.shutdownChan
}

func (pub *defaultPublisher) hostAndPort() (string, string, error) {
	_, port, err := net.SplitHostPort(pub.listener.Addr().String())
	if err != nil {
		// Not reached
		pub.node.log.Error().Msg("failed to split host port")
		return "", "", err
	}
	return pub.node.hostname, port, nil
}

type remoteSubscriberSession struct {
	id                 int
	conn               net.Conn
	nodeID             string
	callerID           string
	topic              string
	typeText           string
	md5sum             string
	typeName           string
	sizeBytesSent      uint32
	msgBytesSent       uint32
	numSent            int64
	quitChan           chan struct{}
	msgChan            chan []byte
	errorChan          chan error
	log                zerolog.Logger
	connectCallback    func(SingleSubscriberPublisher)
	disconnectCallback func(SingleSubscriberPublisher)
}

func newRemoteSubscriberSession(pub *defaultPublisher, id int, conn net.Conn) *remoteSubscriberSession {
	session := new(remoteSubscriberSession)
	session.id = id
	session.conn = conn
	session.nodeID = pub.node.qualifiedName
	session.topic = pub.topic
	session.typeText = pub.msgType.Text()
	session.md5sum = pub.msgType.MD5Sum()
	session.typeName = pub.msgType.Name()
	session.sizeBytesSent = 0
	session.msgBytesSent = 0
	session.numSent = 0
	session.quitChan = make(chan struct{})
	session.msgChan = make(chan []byte, 10)
	session.errorChan = pub.sessionErrorChan
	session.log = pub.node.log
	session.connectCallback = pub.connectCallback
	session.disconnectCallback = pub.disconnectCallback
	return session
}

type singleSubPub struct {
	subName string
	topic   string
	msgChan chan []byte
}

func (ssp *singleSubPub) Publish(msg Message) {
	var buf bytes.Buffer
	_ = msg.Serialize(&buf)
	ssp.msgChan <- buf.Bytes()
}

func (ssp *singleSubPub) GetSubscriberName() string {
	return ssp.subName
}

func (ssp *singleSubPub) GetTopic() string {
	return ssp.topic
}

func (session *remoteSubscriberSession) start() {
	session.log.Debug().Msg("remoteSubscriberSession.start enter")

	ssp := &singleSubPub{
		topic:   session.topic,
		msgChan: session.msgChan,
		// callerID is filled in after header gets read later in this function.
	}

	defer func() {
		session.log.Debug().Msg("remoteSubscriberSession.start exit")

		if session.disconnectCallback != nil {
			session.disconnectCallback(ssp)
		}
	}()
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				session.errorChan <- &remoteSubscriberSessionError{session, e}
			} else {
				e = fmt.Errorf("Unkonwn error value")
				session.errorChan <- &remoteSubscriberSessionError{session, e}
			}
		} else {
			e := fmt.Errorf("Normal exit")
			session.errorChan <- &remoteSubscriberSessionError{session, e}
		}
	}()
	// 1. Read connection header
	headers, err := readConnectionHeader(session.conn)
	if err != nil {
		session.log.Error().Msg("failed to read connection header")
		return
	}
	session.log.Debug().Msg("TCPROS connection header:")
	headerMap := make(map[string]string)
	for _, h := range headers {
		headerMap[h.key] = h.value
		session.log.Debug().Str("header", h.key).Str("value", h.value).Msg("")
	}

	if headerMap["type"] != session.typeName && headerMap["type"] != "*" {
		session.log.Error().Str("topic", session.topic).Str("session-type-name", session.typeName).Str("header-type", headerMap["type"]).Msg("incompatible message type: does not match for topic")
		return
	}

	if headerMap["md5sum"] != session.md5sum && headerMap["md5sum"] != "*" {
		session.log.Error().Str("topic", session.topic).Str("session-md5", session.md5sum).Str("header-md5", headerMap["md5sum"]).Msg("incompatible message md5: does not match for topic")
		return
	}
	session.callerID = headerMap["callerid"]
	ssp.subName = headerMap["callerid"]
	if session.connectCallback != nil {
		go session.connectCallback(ssp)
	}

	// 2. Return reponse header
	var resHeaders []header
	resHeaders = append(resHeaders, header{"message_definition", session.typeText})
	resHeaders = append(resHeaders, header{"callerid", session.nodeID})
	resHeaders = append(resHeaders, header{"latching", "0"})
	resHeaders = append(resHeaders, header{"md5sum", session.md5sum})
	resHeaders = append(resHeaders, header{"topic", session.topic})
	resHeaders = append(resHeaders, header{"type", session.typeName})
	session.log.Debug().Msg("TCPROS response header")
	for _, h := range resHeaders {
		session.log.Debug().Str("header", h.key).Str("value", h.value).Msg("")
	}
	err = writeConnectionHeader(resHeaders, session.conn)
	if err != nil {
		session.log.Error().Msg("failed to write response header")
		return
	}

	// 3. Start sending message
	session.log.Debug().Msg("start sending messages...")
	queueMaxSize := 100
	queue := make(chan []byte, queueMaxSize)
	for {
		//session.log.Debug().Msg("session.remoteSubscriberSession")
		select {
		case msg := <-session.msgChan:
			session.log.Trace().Msg("receive msgChan")
			if len(queue) == queueMaxSize {
				<-queue
			}
			queue <- msg

		case <-session.quitChan:
			session.log.Debug().Msg("receive quitChan")
			return

		case msg := <-queue:
			session.log.Debug().Str("msg", hex.EncodeToString(msg)).Int("count", len(msg)).Msg("writing")
			session.conn.SetDeadline(time.Now().Add(30 * time.Millisecond))
			size := uint32(len(msg))
			if err := binary.Write(session.conn, binary.LittleEndian, size); err != nil {
				if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
					session.log.Debug().Msg("timeout")
					// TODO : Make this trigger a faster reconnect
					return
				} else {
					session.log.Error().Err(err).Msg("")
					return
				}
			}
			session.conn.SetDeadline(time.Now().Add(30 * time.Millisecond))
			if _, err := session.conn.Write(msg); err != nil {
				if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
					session.log.Debug().Msg("timeout")
					return
				} else {
					session.log.Error().Err(err).Msg("")
					return
				}
			}
			session.log.Debug().Str("msg", hex.EncodeToString(msg)).Msg("finished writing")
		}
	}
}
