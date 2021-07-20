package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/redcon"

	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

const (
	PING_COMMAND     = "ping"
	SHUTDONW_COMMAND = "shutdown"
	WATCH_COMMAND    = "watch"
	EXEC_COMMAND     = "exec"
	MULTI_COMMAND    = "multi"
)

type ConnHandle func(conn *redcon.Conn, cmd redcon.Command)
type Server struct {
	sync.RWMutex
	cfg          *config.Config
	http         *http.Server
	red          *redcon.Server
	command      *command.Command
	connHandlers map[string]ConnHandle
	client       *store.Client
	closed       chan bool
	gcWait       *sync.WaitGroup
	gcWorkers    int32
	gcClosed     chan bool
}

func (s *Server) ConnHandle(command string, handler ConnHandle) {
	s.connHandlers[command] = handler
}

func (s *Server) ServeRESP(conn *redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(utils.B2S(cmd.Args[0]))
	if handler, ok := s.connHandlers[command]; ok {
		handler(conn, cmd)
	} else if handler, ok := s.command.TxnHandle[command]; ok {
		s.Handler(conn, cmd, handler.Func)
	} else {
		conn.WriteError("ERR unknown command '" + command + "'")
	}
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:          cfg,
		http:         &http.Server{},
		command:      command.NewCommand(cfg),
		connHandlers: make(map[string]ConnHandle),
		closed:       make(chan bool),
		gcClosed:     make(chan bool),
		gcWait:       &sync.WaitGroup{},
	}

	// s.ConnHandle("detach", s.detach)
	// s.ConnHandle("quit", s.quit)
	// s.ConnHandle(SHUTDONW_COMMAND, s.shutdown)
	s.ConnHandle(WATCH_COMMAND, s.watch)
	s.ConnHandle(EXEC_COMMAND, s.exec)
	s.ConnHandle(MULTI_COMMAND, s.multi)

	s.red = redcon.NewServer("", s.ServeRESP, s.Accept, s.Close)
	return s
}

func (s *Server) Start(httpln, redln net.Listener) {
	err := s.OpenStore()
	if err != nil {
		logrus.Fatalln("OpenStore:", err)
	}

	s.command.Start()

	go s.HttpServe(httpln)
	go s.RedisServe(redln)
	go s.StartGC()
}

func (s *Server) OpenStore() error {
	var err error
	s.client, err = store.Open(&s.cfg.Store)
	return err
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
	s.command.Shutdown()
	select {
	case <-s.gcClosed:
	case <-ctx.Done():
	}
}

func (s *Server) Accept(conn *redcon.Conn) bool {
	logrus.Debugf("Accept %s", conn.RemoteAddr())
	return true
}

func (s *Server) Close(conn *redcon.Conn, err error) {
	logrus.Debugf("Close %s", conn.RemoteAddr())
}
