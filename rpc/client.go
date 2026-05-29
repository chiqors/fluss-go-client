package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chiqors/fluss-client-go/auth"
	"github.com/chiqors/fluss-client-go/codec"
	"github.com/chiqors/fluss-client-go/internal/pbutil"
	iproto "github.com/chiqors/fluss-client-go/internal/proto"
	"github.com/chiqors/fluss-client-go/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Config struct {
	Dialer                *net.Dialer
	ClientSoftwareName    string
	ClientSoftwareVersion string
	RequestTimeout        time.Duration
	Authenticator         auth.Authenticator
}

type Client struct {
	cfg      Config
	mu       sync.Mutex
	conns    map[string]*connection
	requests atomic.Int32
}

type Result struct {
	Message protoreflect.ProtoMessage
}

type pendingCall struct {
	respName string
	ch       chan responseOrError
}

type responseOrError struct {
	msg protoreflect.ProtoMessage
	err error
}

type connection struct {
	addr     string
	cfg      Config
	conn     net.Conn
	writeMu  sync.Mutex
	pending  sync.Map
	versions map[protocol.APIKey]int16
	closed   chan struct{}
}

func NewClient(cfg Config) *Client {
	return &Client{cfg: cfg, conns: map[string]*connection{}}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for addr, conn := range c.conns {
		if closeErr := conn.close(); closeErr != nil && err == nil {
			err = fmt.Errorf("%s: %w", addr, closeErr)
		}
	}
	c.conns = map[string]*connection{}
	return err
}

func (c *Client) Invoke(ctx context.Context, addr string, apiKey protocol.APIKey, reqName, respName string, build func(*dynamicpb.Message) error) (protoreflect.ProtoMessage, error) {
	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	req, err := iproto.NewMessage(reqName)
	if err != nil {
		return nil, err
	}
	if err := build(req); err != nil {
		return nil, err
	}
	return conn.invoke(ctx, apiKey, req, respName, c.requests.Add(1))
}

func (c *Client) getConn(ctx context.Context, addr string) (*connection, error) {
	c.mu.Lock()
	conn := c.conns[addr]
	c.mu.Unlock()
	if conn != nil {
		return conn, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn = c.conns[addr]; conn != nil {
		return conn, nil
	}
	rawConn, err := c.cfg.Dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn = &connection{
		addr:     addr,
		cfg:      c.cfg,
		conn:     rawConn,
		versions: map[protocol.APIKey]int16{},
		closed:   make(chan struct{}),
	}
	go conn.readLoop()
	if err := conn.negotiate(ctx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	if err := conn.authenticate(ctx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *connection) negotiate(ctx context.Context) error {
	msg, err := c.invoke(ctx, protocol.APIVersions, mustMessage("ApiVersionsRequest", func(m *dynamicpb.Message) error {
		if err := pbutil.SetString(m.ProtoReflect(), "client_software_name", c.cfg.ClientSoftwareName); err != nil {
			return err
		}
		return pbutil.SetString(m.ProtoReflect(), "client_software_version", c.cfg.ClientSoftwareVersion)
	}), "ApiVersionsResponse", c.nextRequestID())
	if err != nil {
		return err
	}
	resp := msg.ProtoReflect()
	fd, _ := pbutil.Field(resp.Descriptor(), "api_versions")
	list := resp.Get(fd).List()
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message()
		apiKeyField, _ := pbutil.Field(item.Descriptor(), "api_key")
		maxVersionField, _ := pbutil.Field(item.Descriptor(), "max_version")
		c.versions[protocol.APIKey(item.Get(apiKeyField).Int())] = int16(item.Get(maxVersionField).Int())
	}
	return nil
}

func (c *connection) authenticate(ctx context.Context) error {
	if c.cfg.Authenticator == nil || c.cfg.Authenticator.Protocol() == "none" {
		return nil
	}
	token, err := c.cfg.Authenticator.InitialToken(ctx)
	if err != nil {
		return err
	}
	for {
		msg, err := c.invoke(ctx, protocol.Authenticate, mustMessage("AuthenticateRequest", func(m *dynamicpb.Message) error {
			if err := pbutil.SetString(m.ProtoReflect(), "protocol", c.cfg.Authenticator.Protocol()); err != nil {
				return err
			}
			return pbutil.SetBytes(m.ProtoReflect(), "token", token)
		}), "AuthenticateResponse", c.nextRequestID())
		if err != nil {
			return err
		}
		resp := msg.ProtoReflect()
		fd, _ := pbutil.Field(resp.Descriptor(), "challenge")
		if !resp.Has(fd) {
			return nil
		}
		var ok bool
		token, ok, err = c.cfg.Authenticator.NextToken(ctx, resp.Get(fd).Bytes())
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
}

func (c *connection) nextRequestID() int32 {
	return int32(time.Now().UnixNano() & 0x7fffffff)
}

func (c *connection) invoke(ctx context.Context, apiKey protocol.APIKey, req protoreflect.ProtoMessage, respName string, reqID int32) (protoreflect.ProtoMessage, error) {
	version := c.versions[apiKey]
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	frame := codec.EncodeRequest(codec.RequestFrame{
		APIKey:     apiKey,
		APIVersion: version,
		RequestID:  reqID,
		Payload:    payload,
	})
	call := pendingCall{respName: respName, ch: make(chan responseOrError, 1)}
	c.pending.Store(reqID, call)
	defer c.pending.Delete(reqID)

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline && c.cfg.RequestTimeout > 0 {
		deadline = time.Now().Add(c.cfg.RequestTimeout)
		hasDeadline = true
	}
	if hasDeadline {
		_ = c.conn.SetWriteDeadline(deadline)
	}
	c.writeMu.Lock()
	_, err = c.conn.Write(frame)
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errors.New("fluss: connection closed")
	case result := <-call.ch:
		return result.msg, result.err
	}
}

func (c *connection) readLoop() {
	for {
		frame, err := codec.ReadResponse(c.conn)
		if err != nil {
			c.failAll(err)
			close(c.closed)
			return
		}
		switch frame.Type {
		case protocol.ResponseSuccess:
			c.resolve(frame.RequestID, frame.Payload, "")
		case protocol.ResponseError, protocol.ResponseFailure:
			c.resolveError(frame.RequestID, frame.Payload)
		}
	}
}

func (c *connection) resolve(reqID int32, payload []byte, _ string) {
	value, ok := c.pending.Load(reqID)
	if !ok {
		return
	}
	call := value.(pendingCall)
	msg, err := iproto.NewMessage(call.respName)
	if err == nil {
		err = proto.Unmarshal(payload, msg)
	}
	call.ch <- responseOrError{msg: msg, err: err}
}

func (c *connection) resolveError(reqID int32, payload []byte) {
	msg, err := iproto.NewMessage("ErrorResponse")
	if err != nil {
		c.failAll(err)
		return
	}
	if err = proto.Unmarshal(payload, msg); err != nil {
		c.failAll(err)
		return
	}
	rmsg := msg.ProtoReflect()
	codeField, _ := pbutil.Field(rmsg.Descriptor(), "error_code")
	messageField, _ := pbutil.Field(rmsg.Descriptor(), "error_message")
	apiErr := &protocol.APIError{Code: int32(rmsg.Get(codeField).Int())}
	if rmsg.Has(messageField) {
		apiErr.Message = rmsg.Get(messageField).String()
	}
	if reqID == 0 {
		c.failAll(apiErr)
		return
	}
	value, ok := c.pending.Load(reqID)
	if !ok {
		return
	}
	call := value.(pendingCall)
	call.ch <- responseOrError{err: apiErr}
}

func (c *connection) failAll(err error) {
	c.pending.Range(func(key, value any) bool {
		call := value.(pendingCall)
		call.ch <- responseOrError{err: err}
		c.pending.Delete(key)
		return true
	})
}

func (c *connection) close() error {
	return c.conn.Close()
}

func mustMessage(name string, build func(*dynamicpb.Message) error) *dynamicpb.Message {
	msg, err := iproto.NewMessage(name)
	if err != nil {
		panic(err)
	}
	if err := build(msg); err != nil {
		panic(err)
	}
	return msg
}
