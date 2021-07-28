package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

const (
	PING_COMMAND     = "ping"
	SHUTDONW_COMMAND = "shutdown"
	WATCH_COMMAND    = "watch"
	EXEC_COMMAND     = "exec"
	MULTI_COMMAND    = "multi"
)

type Server struct {
	sync.RWMutex
	cfg     *config.Config
	http    *http.Server
	red     *redcon.Server
	command *command.Command
	closed  chan bool
}

func (s *Server) ServeRESP(conn *redcon.Conn, cmd redcon.Command) {
	comma := strings.ToLower(utils.B2S(cmd.Args[0]))
	if comma != command.AUTH_COMMAND && !conn.Auth {
		if !s.command.Default.NoPass() {
			conn.WriteError(xerror.ErrAuthentication.Error())
			return
		}
	}
	if handler, ok := s.command.ConnHandle[comma]; ok {
		handler.Func(conn, cmd)
	} else if handler, ok := s.command.TxnHandle[comma]; ok {
		s.command.TxnHandler(conn, cmd, handler.Func)
	} else {
		conn.WriteError("ERR unknown command '" + comma + "'")
	}
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:     cfg,
		http:    &http.Server{},
		command: command.NewCommand(cfg),
		closed:  make(chan bool),
	}

	s.red = redcon.NewServer("", s.ServeRESP, s.Accept, s.Close)
	return s
}

func (s *Server) Start(httpln, redln net.Listener) {
	err := s.command.Start()
	if err != nil {
		logrus.Fatalln("Server:", err)
	}

	go s.HttpServe(httpln)
	go s.RedisServe(redln)
}

func (s *Server) HttpServe(ln net.Listener) {
	err := s.http.Serve(ln)
	if err != http.ErrServerClosed {
		logrus.Errorf("HTTP Server: %s", err)
	}
}

func (s *Server) RedisServe(ln net.Listener) {
	err := s.red.Serve(ln)
	if err != nil {
		logrus.Errorf("Redis Server: %s", err)
	}
}

func (s *Server) Shutdown(ctx context.Context) {
	close(s.closed)
	_ = s.red.Close(ctx)
	_ = s.http.Shutdown(ctx)
	s.command.Shutdown(ctx)
}

func (s *Server) Accept(conn *redcon.Conn) bool {
	logrus.Debugf("Accept %s", conn.RemoteAddr())
	return true
}

func (s *Server) Close(conn *redcon.Conn, err error) {
	logrus.Debugf("Close %s", conn.RemoteAddr())
}
