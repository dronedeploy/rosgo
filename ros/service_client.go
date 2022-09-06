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
	"github.com/rs/zerolog"
	"github.com/team-rocos/rosgo/xmlrpc"
)

const headerReadTimeout time.Duration = 1000 * time.Millisecond
const okReplyTimeout time.Duration = 1000 * time.Millisecond
const responseTimeout time.Duration = 5000 * time.Millisecond
const responseBaseTimeout time.Duration = 1000 * time.Millisecond
const responseByteMultiplier time.Duration = time.Millisecond

type defaultServiceClient struct {
	logger    zerolog.Logger
	service   string
	srvType   ServiceType
	masterURI string
	nodeID    string
	xmlClient *xmlrpc.XMLClient
}

func newDefaultServiceClient(log zerolog.Logger, nodeID string, masterURI string, service string, srvType ServiceType) *defaultServiceClient {
	client := new(defaultServiceClient)
	client.logger = log
	client.service = service
	client.srvType = srvType
	client.masterURI = masterURI
	client.nodeID = nodeID
	client.xmlClient = xmlrpc.NewXMLClient()
	client.xmlClient.Timeout = defaultMasterAPITimeout
	return client
}

func (c *defaultServiceClient) Call(srv Service) error {
	c.logger.Debug().Str("service", c.service).Msg("calling service: looking up service URI...")
	result, err := callRosAPI(c.xmlClient, c.masterURI, "lookupService", c.nodeID, c.service)
	if err != nil {
		return err
	}
	c.logger.Debug().Str("service", c.service).Str("uri", result.(string)).Msg("calling service: got service URI")

	serviceRawURL, converted := result.(string)
	if !converted {
		return fmt.Errorf("result of 'lookupService' is not a string")
	}
	var serviceURL *url.URL
	serviceURL, err = url.Parse(serviceRawURL)
	if err != nil {
		return err
	}

	return c.doServiceRequest(srv, serviceURL.Host)
}

func (c *defaultServiceClient) doServiceRequest(srv Service, serviceURI string) error {

	c.logger.Debug().Str("service", c.service).Str("uri", serviceURI).Msg("resolving...")
	host, port, err := net.SplitHostPort(serviceURI)
	if err != nil {
		return err
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return err
	}
	if len(addrs) <= 1 {
		return errors.New("failed to resolve")
	}

	c.logger.Debug().Str("service", c.service).Str("address", addrs[0]).Msg("dialling...")
	var conn net.Conn
	conn, err = net.Dial("tcp", addrs[0]+":"+port)
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
	c.logger.Debug().Msg("TCPROS connection header")
	for _, h := range headers {
		c.logger.Debug().Str("header", h.key).Str("value", h.value).Msg("")
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
	c.logger.Debug().Msg("TCPROS response header:")
	resHeaderMap := make(map[string]string)
	for _, h := range resHeaders {
		resHeaderMap[h.key] = h.value
		c.logger.Debug().Str("header", h.key).Str("value", h.value).Msg("")
	}
	if resHeaderMap["type"] != msgType || resHeaderMap["md5sum"] != md5sum {
		err = errors.New("incompatible message type")
		return err
	}
	c.logger.Debug().Msg("start receiving messages...")

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
	c.logger.Debug().Uint32("size", size).Msg("sent request with size")
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
	c.logger.Debug().Uint32("message-size", msgSize).Msg("read response")
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
