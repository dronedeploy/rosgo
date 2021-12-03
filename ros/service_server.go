package ros

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"
)

type serviceResult struct {
	srv Service
	err error
}

type remoteClientSessionCloseEvent struct {
	session *remoteClientSession
	err     error
}

type defaultServiceServer struct {
	node             *defaultNode
	service          string
	srvType          ServiceType
	handler          interface{}
	listener         *net.TCPListener
	rosrpcAddr       string
	sessions         *list.List
	shutdownChan     chan struct{}
	sessionCloseChan chan *remoteClientSessionCloseEvent
}

func newDefaultServiceServer(node *defaultNode, service string, srvType ServiceType, handler interface{}) *defaultServiceServer {
	logger := node.log
	server := new(defaultServiceServer)
	if listener, err := listenRandomPort(node.listenIP, 10); err != nil {
		logger.Error().Err(err).Msg("failed to listen to random port")
		return nil
	} else {
		if tcpListener, ok := listener.(*net.TCPListener); ok {
			server.listener = tcpListener
		} else {
			logger.Error().Msg("server listener is not TCPListener")
			return nil
		}
	}
	server.node = node
	server.service = service
	server.srvType = srvType
	server.handler = handler
	server.sessions = list.New()
	server.shutdownChan = make(chan struct{}, 10)
	server.sessionCloseChan = make(chan *remoteClientSessionCloseEvent, 10)
	_, port, err := net.SplitHostPort(server.listener.Addr().String())
	if err != nil {
		// Not reached
		logger.Error().Err(err).Msg("failed to split host port")
		return nil
	}
	server.rosrpcAddr = fmt.Sprintf("rosrpc://%s:%s", node.hostname, port)
	logger.Debug().Str("address", server.rosrpcAddr).Msg("ServiceServer listen")
	_, err = callRosAPI(node.xmlClient, node.masterURI, "registerService",
		node.qualifiedName,
		service,
		server.rosrpcAddr,
		node.xmlrpcURI)
	if err != nil {
		logger.Error().Str("service", service).Msg("failed to register service")
		server.listener.Close()
		return nil
	}
	go server.start()
	return server
}

func (s *defaultServiceServer) Shutdown() {
	s.shutdownChan <- struct{}{}
}

// event loop
func (s *defaultServiceServer) start() {
	logger := s.node.log
	logger.Debug().Str("service", s.service).Str("address", s.listener.Addr().String()).Msg("service server start listening")
	s.node.waitGroup.Add(1)
	defer func() {
		logger.Debug().Msg("defaultServiceServer.start exit")
		s.node.waitGroup.Done()
	}()

	for {
		//logger.Debug().Msg("defaultServiceServer.start loop");
		s.listener.SetDeadline(time.Now().Add(1 * time.Millisecond))
		if conn, err := s.listener.Accept(); err != nil {
			opError, ok := err.(*net.OpError)
			if !ok || !opError.Timeout() {
				logger.Debug().Msg("s.listner.Accept() failed")
				return
			}
		} else {
			logger.Debug().Str("address", conn.RemoteAddr().String()).Msg("connected")
			session := newRemoteClientSession(s, conn)
			s.sessions.PushBack(session)
			go session.start()
		}

		timeoutChan := time.After(1 * time.Millisecond)
		select {
		case ev := <-s.sessionCloseChan:
			if ev.err != nil {
				logger.Error().Err(ev.err).Msg("session error")
			}
			for e := s.sessions.Front(); e != nil; e = e.Next() {
				if e.Value == ev.session {
					logger.Debug().Interface("session", e.Value).Msg("service session removed")
					s.sessions.Remove(e)
					break
				}
			}
		case <-s.shutdownChan:
			logger.Debug().Msg("defaultServiceServer.start Receive shutdownChan")
			s.listener.Close()
			logger.Debug().Msg("defaultServiceServer.start closed listener")
			_, err := callRosAPI(s.node.xmlClient, s.node.masterURI, "unregisterService",
				s.node.qualifiedName, s.service, s.rosrpcAddr)
			if err != nil {
				logger.Warn().Str("service", s.service).Err(err).Msg("failed unregisterService")
			}
			logger.Debug().Str("service", s.service).Msg("called unregisterService")
			for e := s.sessions.Front(); e != nil; e = e.Next() {
				session := e.Value.(*remoteClientSession)
				session.quitChan <- struct{}{}
			}
			s.sessions.Init() // Clear all sessions
			logger.Debug().Msg("defaultServiceServer.start session cleared")
			return
		case <-timeoutChan:
			break
		}
	}
}

type remoteClientSession struct {
	server       *defaultServiceServer
	conn         net.Conn
	quitChan     chan struct{}
	responseChan chan []byte
	errorChan    chan error
}

func newRemoteClientSession(s *defaultServiceServer, conn net.Conn) *remoteClientSession {
	session := new(remoteClientSession)
	session.server = s
	session.conn = conn
	session.responseChan = make(chan []byte)
	session.errorChan = make(chan error)
	return session
}

func (s *remoteClientSession) start() {
	logger := s.server.node.log
	conn := s.conn
	nodeID := s.server.node.qualifiedName
	service := s.server.service
	md5sum := s.server.srvType.MD5Sum()
	srvType := s.server.srvType.Name()
	var err error
	logger.Debug().Str("service", s.server.service).Msg("remoteClientSession.start")
	defer func() {
		logger.Debug().Msg("remoteClientSession.start exit")
	}()
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				e = fmt.Errorf("remoteClientSession %v error: %v", s, e)
				s.server.sessionCloseChan <- &remoteClientSessionCloseEvent{s, e}
			} else {
				e = fmt.Errorf("remoteClientSession %v error: Unkonwn error value", s)
				s.server.sessionCloseChan <- &remoteClientSessionCloseEvent{s, e}
			}
		} else {
			s.server.sessionCloseChan <- &remoteClientSessionCloseEvent{s, nil}
		}
	}()

	// 1. Read request header
	conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
	reqHeader, err := readConnectionHeader(conn)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read connection header")
		return
	}
	reqHeaderMap := make(map[string]string)
	for _, h := range reqHeader {
		reqHeaderMap[h.key] = h.value
		logger.Debug().Str("header", h.key).Str("value", h.value).Msg("")
	}

	// 2. Write response header
	var headers []header
	headers = append(headers, header{"service", service})
	headers = append(headers, header{"md5sum", md5sum})
	headers = append(headers, header{"type", srvType})
	headers = append(headers, header{"callerid", nodeID})
	logger.Debug().Msg("TCPROS response header")
	for _, h := range headers {
		logger.Debug().Str("header", h.key).Str("value", h.value).Msg("")
	}
	conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
	if err := writeConnectionHeader(headers, conn); err != nil {
		logger.Error().Err(err).Msg("failed to write connection header")
		return
	}

	if probe, ok := reqHeaderMap["probe"]; ok && probe == "1" {
		logger.Debug().Msg("TCPROS header 'probe' detected. session closed")
		return
	}
	if reqHeaderMap["service"] != service ||
		reqHeaderMap["md5sum"] != md5sum {
		logger.Error().Msg("incompatible message type!")
		return
	}

	// 3. Read request
	logger.Debug().Msg("reading message size...")
	var msgSize uint32
	conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
	if err := binary.Read(conn, binary.LittleEndian, &msgSize); err != nil {
		panic(err)
	}
	logger.Debug().Uint32("message-size", msgSize).Msg("")
	resBuffer := make([]byte, int(msgSize))
	logger.Debug().Msg("reading message body...")
	conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err = io.ReadFull(conn, resBuffer); err != nil {
		panic(err)
	}

	s.server.node.jobChan <- func() {
		srv := s.server.srvType.NewService()
		reader := bytes.NewReader(resBuffer)
		err := srv.ReqMessage().Deserialize(reader)
		if err != nil {
			s.errorChan <- err
		}
		args := []reflect.Value{reflect.ValueOf(srv)}
		fun := reflect.ValueOf(s.server.handler)
		results := fun.Call(args)

		if len(results) != 1 {
			logger.Debug().Msg("service callback return type must be 'error'")
			s.errorChan <- err
			return
		}
		result := results[0]
		if result.IsNil() {
			logger.Debug().Msg("service callback success")
			var buf bytes.Buffer
			_ = srv.ResMessage().Serialize(&buf)
			s.responseChan <- buf.Bytes()
		} else {
			logger.Debug().Msg("service callback failure")
			if err, ok := result.Interface().(error); ok {
				s.errorChan <- err
			} else {
				s.errorChan <- fmt.Errorf("Service handler has invalid signature")
			}
		}
	}

	timeoutChan := time.After(1000 * time.Millisecond)
	select {
	case resMsg := <-s.responseChan:
		// 4. Write OK byte
		var ok byte = 1
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err := binary.Write(conn, binary.LittleEndian, &ok); err != nil {
			panic(err)
		}
		// 5. Write response
		logger.Debug().Int("response-size", len(resMsg)).Msg("")
		size := uint32(len(resMsg))
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err := binary.Write(conn, binary.LittleEndian, size); err != nil {
			panic(err)
		}
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if _, err := conn.Write(resMsg); err != nil {
			panic(err)
		}
	case err := <-s.errorChan:
		logger.Error().Err(err).Msg("")
		// 4. Write OK byte
		var ok byte
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err := binary.Write(conn, binary.LittleEndian, &ok); err != nil {
			panic(err)
		}
		errMsg := err.Error()
		size := uint32(len(errMsg))
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err := binary.Write(conn, binary.LittleEndian, size); err != nil {
			panic(err)
		}
		conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if _, err := conn.Write([]byte(errMsg)); err != nil {
			panic(err)
		}
	case <-timeoutChan:
		panic(fmt.Errorf("service callback timeout"))
	}
}
