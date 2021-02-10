package ros

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/pkg/errors"

	modular "github.com/edwinhayes/logrus-modular"
)

const headerReadTimeout time.Duration = 1000 * time.Millisecond
const okReplyTimeout time.Duration = 1000 * time.Millisecond
const responseTimeout time.Duration = 5000 * time.Millisecond
const responseBaseTimeout time.Duration = 1000 * time.Millisecond
const responseByteMultiplier time.Duration = time.Millisecond

type defaultServiceClient struct {
	logger    *modular.ModuleLogger
	service   string
	srvType   ServiceType
	masterURI string
	nodeID    string
}

func newDefaultServiceClient(log *modular.ModuleLogger, nodeID string, masterURI string, service string, srvType ServiceType) *defaultServiceClient {
	client := new(defaultServiceClient)
	client.logger = log
	client.service = service
	client.srvType = srvType
	client.masterURI = masterURI
	client.nodeID = nodeID
	return client
}

func (c *defaultServiceClient) Call(srv Service) error {

	result, err := callRosAPI(c.masterURI, "lookupService", c.nodeID, c.service)
	if err != nil {
		return err
	}

	serviceRawURL, converted := result.(string)
	if !converted {
		return fmt.Errorf("Result of 'lookupService' is not a string")
	}
	var serviceURL *url.URL
	serviceURL, err = url.Parse(serviceRawURL)
	if err != nil {
		return err
	}

	return c.doServiceRequest(srv, serviceURL.Host)
}

func (c *defaultServiceClient) doServiceRequest(srv Service, serviceURI string) error {
	logger := *c.logger

	var conn net.Conn
	var err error
	conn, err = net.Dial("tcp", serviceURI)
	if err != nil {
		return err
	}

	// 1. Write connection header
	var headers []header
	md5sum := c.srvType.MD5Sum()
	msgType := c.srvType.Name()
	headers = append(headers, header{"service", c.service})
	headers = append(headers, header{"md5sum", md5sum})
	headers = append(headers, header{"type", msgType})
	headers = append(headers, header{"callerid", c.nodeID})
	logger.Debug("TCPROS Connection Header")
	for _, h := range headers {
		logger.Debugf("  `%s` = `%s`", h.key, h.value)
	}
	if err := writeConnectionHeader(headers, conn); err != nil {
		return err
	}

	// 2. Read reponse header
	conn.SetReadDeadline(time.Now().Add(headerReadTimeout))
	resHeaders, err := readConnectionHeader(conn)
	if err != nil {
		return err
	}
	logger.Debug("TCPROS Response Header:")
	resHeaderMap := make(map[string]string)
	for _, h := range resHeaders {
		resHeaderMap[h.key] = h.value
		logger.Debugf("  `%s` = `%s`", h.key, h.value)
	}
	if resHeaderMap["type"] != msgType || resHeaderMap["md5sum"] != md5sum {
		err = errors.New("incompatible message type")
		return err
	}
	logger.Debug("Start receiving messages...")

	// 3. Send request
	var buf bytes.Buffer
	err = srv.ReqMessage().Serialize(&buf)
	if err != nil {
		return errors.Wrap(err, "service call failed to serialize")
	}
	reqMsg := buf.Bytes()
	size := uint32(len(reqMsg))
	if err := binary.Write(conn, binary.LittleEndian, size); err != nil {
		return err
	}
	logger.Debugf("sent request, length: %d", size)
	if _, err := conn.Write(reqMsg); err != nil {
		return err
	}

	// 4. Read OK byte
	var ok byte
	conn.SetReadDeadline(time.Now().Add(okReplyTimeout))
	if err := binary.Read(conn, binary.LittleEndian, &ok); err != nil {
		return err
	}
	if ok == 0 {
		var size uint32
		conn.SetDeadline(time.Now().Add(responseTimeout))
		if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
			return err
		}
		errMsg := make([]byte, int(size))
		conn.SetDeadline(time.Now().Add(responseBaseTimeout).Add(responseByteMultiplier * time.Duration(size)))

		if _, err := io.ReadFull(conn, errMsg); err != nil {
			return err
		}
		return errors.New(string(errMsg))
	}

	// 5. Receive response
	conn.SetDeadline(time.Now().Add(responseTimeout))
	var msgSize uint32
	if err := binary.Read(conn, binary.LittleEndian, &msgSize); err != nil {
		return err
	}
	logger.Debugf("Message Size:  %d", msgSize)
	resBuffer := make([]byte, int(msgSize))
	conn.SetDeadline(time.Now().Add(responseBaseTimeout).Add(responseByteMultiplier * time.Duration(msgSize)))
	if _, err = io.ReadFull(conn, resBuffer); err != nil {
		return err
	}
	resReader := bytes.NewReader(resBuffer)
	if err := srv.ResMessage().Deserialize(resReader); err != nil {
		return err
	}
	return nil
}

func (*defaultServiceClient) Shutdown() {}
