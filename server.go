package nfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"time"
)

// Server is a handle to the listening NFS server.
type Server struct {
	Handler
	ID [8]byte
	context.Context
}

// RegisterMessageHandler registers a handler for a specific XDR procedure.
func RegisterMessageHandler(protocol uint32, proc uint32, handler HandleFunc) error {
	if registeredHandlers == nil {
		registeredHandlers = make(map[HandlerID]HandleFunc)
	}
	for k := range registeredHandlers {
		if k.protocol == protocol && k.proc == proc {
			return errors.New("already registered")
		}
	}
	id := HandlerID{protocol, proc}
	registeredHandlers[id] = handler
	return nil
}

// HandleFunc represents a handler for a specific protocol message.
type HandleFunc func(ctx context.Context, w *response, userHandler Handler) error

type HandleContext struct {
	ID HandlerID
	Fn HandleFunc
}

type HandlerID struct {
	protocol uint32
	proc     uint32
}

func (id HandlerID) String() string {
	var svc string
	switch id.protocol {
	case mountServiceID:
		svc = "mount"
	case nfsServiceID:
		svc = "nfs"
	default:
		svc = fmt.Sprintf("%v", id.protocol)
	}
	return fmt.Sprintf("%s %s", svc, NFSProcedure(id.proc).String())
}

var registeredHandlers map[HandlerID]HandleFunc

// Serve listens on the provided listener port for incoming client requests.
func (s *Server) Serve(l net.Listener) error {
	defer l.Close()
	baseCtx := context.Background()
	if s.Context != nil {
		baseCtx = s.Context
	}
	if bytes.Equal(s.ID[:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		if _, err := rand.Reader.Read(s.ID[:]); err != nil {
			return err
		}
	}

	var tempDelay time.Duration

	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0
		c := s.newConn(conn)
		go c.serve(baseCtx)
	}
}

func (s *Server) newConn(nc net.Conn) *conn {
	c := &conn{
		Server: s,
		Conn:   nc,
	}
	return c
}

// TODO: keep an immutable map for each server instance to have less
// chance of races.
func (s *Server) handlerFor(prog uint32, proc uint32) *HandleContext {
	for k, v := range registeredHandlers {
		if k.protocol == prog && k.proc == proc {
			return &HandleContext{
				ID: HandlerID{
					protocol: prog,
					proc:     proc,
				},
				Fn: v,
			}
		}
	}
	return nil
}

// Serve is a singleton listener paralleling http.Serve
func Serve(l net.Listener, handler Handler) error {
	srv := &Server{Handler: handler}
	return srv.Serve(l)
}
