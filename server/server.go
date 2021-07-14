package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"

	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

const (
	PING_COMMAND     = "ping"
	SHUTDONW_COMMAND = "shutdown"
	WATCH_COMMAND    = "watch"
	EXEC_COMMAND     = "exec"
	MULTI_COMMAND    = "multi"
)

type TxnHandle func(txn *store.Txn, args [][]byte) store.RespFunc
type ConnHandle func(txn redcon.Conn, cmd redcon.Command)
type Server struct {
	sync.RWMutex
	cfg          *config.Config
	http         *http.Server
	red          *redcon.Server
	txnHandlers  map[string]TxnHandle
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

func (s *Server) TxnHandle(command string, handler TxnHandle) {
	s.txnHandlers[command] = handler
}

func (s *Server) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	if handler, ok := s.connHandlers[command]; ok {
		handler(conn, cmd)
	} else if handler, ok := s.txnHandlers[command]; ok {
		s.Handler(conn, cmd, handler)
	} else {
		conn.WriteError("ERR unknown command '" + command + "'")
	}
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:          cfg,
		http:         &http.Server{},
		txnHandlers:  make(map[string]TxnHandle),
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

	s.TxnHandle(command.GET_COMMAND, command.GetHandle)
	s.TxnHandle(command.SET_COMMAND, command.SetHandle)
	s.TxnHandle(command.TTL_COMMAND, command.TTLHandle)
	s.TxnHandle(command.HSET_COMMAND, command.HSetHandle)
	s.TxnHandle(command.HGET_COMMAND, command.HGetHandle)

	s.red = redcon.NewServer("", s.ServeRESP, s.Accept, s.Close)
	return s
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
	select {
	case <-s.gcClosed:
	case <-ctx.Done():
	}
}

// func (s *Server) Handle(conn redcon.Conn, cmd redcon.Command) {
// 	logrus.Debugf("cmd: %s", cmd)
// 	switch strings.ToLower(string(cmd.Args[0])) {
// 	default:
// 		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
// 	case "ping":
// 		conn.WriteString("PONG")
// 	case "quit":
// 		conn.WriteString("OK")
// 		conn.Close()
// 	case "shutdown":
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
// 		defer cancel()
// 		s.Shutdown(ctx)
// 	}
// }

func (s *Server) Accept(conn redcon.Conn) bool {
	logrus.Debugf("Accept %s", conn.RemoteAddr())
	return true
}

func (s *Server) Close(conn redcon.Conn, err error) {
	logrus.Debugf("Close %s", conn.RemoteAddr())
}
