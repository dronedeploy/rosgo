package ros

import (
	"bytes"
	goContext "context"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog"
)

// defaultSubscription connects to a publisher and runs a go routine to maintain its connection and packetize messages from the tcp stream. Messages are passed through the messageChan channel.
type defaultSubscription struct {
	pubURI                 string
	topic                  string
	msgType                MessageType
	nodeID                 string
	messageChan            chan messageEvent
	remoteDisconnectedChan chan string // Outbound signal to indicate a disconnected channel.
	event                  MessageEvent
	dialer                 TCPRosDialer
}

// newDefaultSubscription populates a subscription struct from the instantiation fields and fills in default data for the operational fields.
func newDefaultSubscription(
	pubURI string, topic string, msgType MessageType, nodeID string,
	messageChan chan messageEvent,
	remoteDisconnectedChan chan string) *defaultSubscription {

	return &defaultSubscription{
		pubURI:                 pubURI,
		topic:                  topic,
		msgType:                msgType,
		nodeID:                 nodeID,
		messageChan:            messageChan,
		remoteDisconnectedChan: remoteDisconnectedChan,
		event:                  MessageEvent{"", time.Time{}, nil},
		dialer:                 &TCPRosNetDialer{},
	}
}

// readResult determines the result of a subscription read operation.
type readResult int

const (
	readResultOk readResult = iota
	readResultError
	readResultDisconnected
	readResultResync
	readResultCancel
)

// start spawns a go routine which connects a subscription to a publisher.
func (s *defaultSubscription) start(log zerolog.Logger) {
	go s.run(goContext.Background(), log) // TODO Remove this function, rename the other to start
}

// start spawns a go routine which connects a subscription to a publisher.
func (s *defaultSubscription) startWithContext(ctx goContext.Context, log zerolog.Logger) {
	go s.run(ctx, log)
}

// run connects to a publisher and attempts to maintain a connection until either a stop is requested or the publisher disconnects.
func (s *defaultSubscription) run(ctx goContext.Context, log zerolog.Logger) {

	logger := log
	logger.Debug().Str("topic", s.topic).Msg("defaultSubscription.run() has started")

	defer func() {
		logger.Debug().Str("topic", s.topic).Msg("defaultSubscription.run() has exited")
	}()

	var conn net.Conn

	// The recovery loop: if a connection to the publisher fails or goes out of sync, this loop allows us to attempt to start again with a new subscription.
	for {
		// Establish a connection with our publisher.
		if s.connectToPublisher(ctx, &conn, log) == false {
			if conn != nil {
				conn.Close()
			}
			logger.Info().Str("topic", s.topic).Msg("could not connect to publisher, closing connection")
			return
		}

		// Reading from publisher, this will only return when our connection fails.
		result := s.readFromPublisher(ctx, conn, log)

		// Under healthy conditions, we don't get here. Always close the connection, then handle the returned connection state.
		conn.Close()
		conn = nil

		switch result {
		case readResultResync: // TCP out of sync; we will attempt to resync by closing the connection and trying again.
			logger.Debug().Str("topic", s.topic).Msg("connection closed - attempting to reconnect with publisher")
			continue
		case readResultCancel: // Cancelled - easy, just return!
			return
		case readResultDisconnected: // Publisher disconnected - not much we can do here, the subscription has ended.
			logger.Info().Str("topic", s.topic).Str("pubURI", s.pubURI).Msg("connection closed - publisher has disconnected")
			s.remoteDisconnectedChan <- s.pubURI
			return
		case readResultError: // Read failure; the reason is uncertain, maybe the bus is polluted? We give up.
			logger.Error().Str("topic", s.topic).Msg("connection closed - failed to read a message correctly")
			s.remoteDisconnectedChan <- s.pubURI
			return
		default: // Unknown failure - this is a bug, log the connectionFailureMode to help determine cause.
			logger.Error().Str("topic", s.topic).Int("readResult", int(result)).Msg("connection closed - unknown failure mode")
			return
		}
	}
}

// connectToPublisher estabilishes a TCPROS connection with a publishing node by exchanging headers to ensure both nodes are using the same message type.
func (s *defaultSubscription) connectToPublisher(ctx goContext.Context, conn *net.Conn, log zerolog.Logger) bool {
	var err error

	logger := log

	var subscriberHeaders []header
	subscriberHeaders = append(subscriberHeaders, header{"topic", s.topic})
	subscriberHeaders = append(subscriberHeaders, header{"md5sum", s.msgType.MD5Sum()})
	subscriberHeaders = append(subscriberHeaders, header{"type", s.msgType.Name()})
	subscriberHeaders = append(subscriberHeaders, header{"callerid", s.nodeID})

	ctx, cancel := goContext.WithCancel(ctx)
	defer cancel()

	// 1. Connnect to tcp.
	if *conn, err = s.dialer.Dial(ctx, s.pubURI); err != nil {
		logger.Debug().Str("topic", s.topic).Str("pubURI", s.pubURI).Err(err).Msg("failed to dial publisher")
		return false
	}

	// 2. Write connection header to the publisher.
	if err = s.writeHeader(ctx, conn, log, subscriberHeaders); err != nil {
		logger.Error().Str("topic", s.topic).Err(err).Msg("failed to write connection header")
		return false
	}

	// Return if stop requested.
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// 3. Read the publisher's reponse header.
	var resHeaderMap map[string]string
	if resHeaderMap, err = s.readHeader(ctx, conn, log); err != nil {
		logger.Error().Str("topic", s.topic).Err(err).Msg("failed to write connection header")
		return false
	}

	// Return if stop requested.
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// 4. Verify the publisher's response header.
	if resHeaderMap["type"] != s.msgType.Name() || resHeaderMap["md5sum"] != s.msgType.MD5Sum() {
		logger.Error().Interface("pubs", resHeaderMap).Interface("subs", subscriberHeaders).Msg("publisher provided incompatable message header")
		return false
	}

	// Some incomplete TCPROS implementations do not include topic name in response; set it if it is currently empty.
	if resHeaderMap["topic"] == "" {
		resHeaderMap["topic"] = s.topic
	}

	// Construct the event struct to be sent with each message.
	s.event = MessageEvent{
		PublisherName:    resHeaderMap["callerid"],
		ConnectionHeader: resHeaderMap,
	}
	return true
}

func (s *defaultSubscription) writeHeader(ctx goContext.Context, conn *net.Conn, log zerolog.Logger, subscriberHeaders []header) (err error) {
	logger := log
	logger.Debug().Interface("sub-headers", subscriberHeaders).Msg("writing TCPROS connection header")

	headerWriter := bytes.NewBuffer(make([]byte, 0))
	err = writeConnectionHeader(subscriberHeaders, headerWriter)
	if err != nil {
		return err
	}

	// Write the TCPROS message.
	ctx, cancel := goContext.WithCancel(ctx)
	defer cancel()

	writeResultChan := make(chan error)
	go writeTCPRosMessage(ctx, *conn, headerWriter.Bytes()[4:], writeResultChan)

	select {
	case <-ctx.Done():
		return nil
	case err := <-writeResultChan:
		return err
	}
}

func (s *defaultSubscription) readHeader(ctx goContext.Context, conn *net.Conn, log zerolog.Logger) (resHeaderMap map[string]string, err error) {
	logger := log

	// Read a TCPROS message.
	ctx, cancel := goContext.WithCancel(ctx)
	defer cancel()

	readResultChan := make(chan TCPRosReadResult)
	go readTCPRosMessage(ctx, *conn, readResultChan)

	var headerReader *bytes.Reader
	var headerSize uint32
	select {
	case result := <-readResultChan:
		if result.Err != nil {
			return nil, result.Err
		}
		headerReader = bytes.NewReader(result.Buf)
		headerSize = uint32(len(result.Buf))
	case <-ctx.Done():
		return nil, nil
	}

	var resHeaders []header
	resHeaders, err = readConnectionHeaderPayload(headerReader, headerSize)
	if err != nil {
		logger.Error().Str("topic", s.topic).Err(err).Msg("failed to read response header")
		return nil, err
	}

	resHeaderMap = make(map[string]string)
	for _, h := range resHeaders {
		resHeaderMap[h.key] = h.value
	}
	logger.Debug().Str("topic", s.topic).Interface("resp-headers", resHeaders).Msg("received TCPROS response header")
	return resHeaderMap, err
}

// readFromPublisher maintains a connection with a publisher. When a connection is stable, it will loop until either the publisher or subscriber disconnects.
func (s *defaultSubscription) readFromPublisher(ctx goContext.Context, conn net.Conn, log zerolog.Logger) readResult {
	logger := log

	// TCPROS reader setup.
	ctx, cancel := goContext.WithCancel(ctx)
	defer cancel()
	readResultChan := make(chan TCPRosReadResult)

	// Read messages until the context is done.
	go func() {
		for {
			readTCPRosMessage(ctx, conn, readResultChan)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// Control and prioritize messages
	// activeMsgChan stays nil until there is a new message to forward
	// latestMessage always holds the latest message avaliable
	var activeMsgChan chan messageEvent
	var latestMessage messageEvent

	// Subscriber loop:
	// - Packages the tcp serial stream into messages and passes them through the message channel.
	// - Prioritizes the latest message, discards stale messages using the nil channel pattern.
	//   https://www.godesignpatterns.com/2014/05/nil-channels-always-block.html
	// - Uses context for cancellation.
	for {
		select {
		case tcpResult := <-readResultChan:
			rResult := errorToReadResult(tcpResult.Err)
			switch rResult {
			case readResultOk:
				if activeMsgChan != nil {
					logger.Trace().Str("topic", s.topic).Msg("stale message dropped")
				}
				s.event.ReceiptTime = time.Now()
				latestMessage = messageEvent{bytes: tcpResult.Buf, event: s.event}
				activeMsgChan = s.messageChan
			default:
				// We aren't ok, return.
				return rResult
			}
		case activeMsgChan <- latestMessage:
			activeMsgChan = nil
			latestMessage = messageEvent{}
		case <-ctx.Done():
			return readResultCancel
		}
	}
}

// errorToReadResult converts errors to readResult to be handled further up the callstack.
func errorToReadResult(err error) readResult {
	if err == nil {
		return readResultOk
	}
	if err == io.EOF {
		return readResultDisconnected
	}
	if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		return readResultError
	}
	if e, ok := err.(*TCPRosError); ok {
		// We assume if the size is too large, we have gone out of sync.
		if *e == TCPRosErrorSizeTooLarge {
			return readResultResync
		}
	}
	// Not sure what the cause was - it is just a generic error.
	return readResultError
}
