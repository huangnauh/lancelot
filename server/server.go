package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/grpc"
	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
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
	rpc     *grpc.Server
	Command *command.Command
	closed  chan bool
}

func (s *Server) ServeRESP(conn *redcon.Conn, cmd redcon.Command) {
	comma := strings.ToLower(utils.B2S(cmd.Args[0]))
	if comma != command.AUTH_COMMAND && !conn.Auth {
		if !s.Command.Default.NoPass() {
			conn.WriteError(xerror.ErrAuthentication.Error())
			return
		}
	}
	if handler, ok := s.Command.ConnHandle[comma]; ok {
		utils.ZapLog.Debug("ConnHandle", zap.String("remote", conn.RemoteAddr()),
			zap.ByteStrings("args", cmd.Args))
		handler.Func(conn, cmd)
	} else if handler, ok := s.Command.TxnHandle[comma]; ok {
		s.Command.TxnHandler(conn, comma, cmd, handler.Func)
	} else {
		conn.WriteError("ERR unknown command '" + comma + "'")
	}
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:     cfg,
		http:    &http.Server{},
		Command: command.NewCommand(cfg),
		closed:  make(chan bool),
	}
	s.rpc = grpc.NewGrpcServer(cfg)
	lancepb.RegisterLanceServer(s.rpc.GRPCServer, s.Command)
	s.red = redcon.NewServer("", s.ServeRESP, s.Accept, s.Close)
	return s
}

func (s *Server) Start(httpln, redln, rpcln net.Listener) {
	err := s.Command.Start()
	if err != nil {
		utils.ZapLog.Fatal("Server Start", zap.Error(err))
	}

	go s.HttpServe(httpln)
	go s.RedisServe(redln)
	go s.GrpcServe(rpcln)
}

func (s *Server) HttpServe(ln net.Listener) {
	err := s.http.Serve(ln)
	if err != http.ErrServerClosed {
		utils.ZapLog.Error("HTTP Serve", zap.Error(err))
	}
}

func (s *Server) GrpcServe(ln net.Listener) {
	err := s.rpc.GRPCServer.Serve(ln)
	if err != nil {
		utils.ZapLog.Error("Grpc Serve", zap.Error(err))
	}
}

func (s *Server) RedisServe(ln net.Listener) {
	err := s.red.Serve(ln)
	if err != nil {
		utils.ZapLog.Error("Redis Serve", zap.Error(err))
	}
}

func (s *Server) Shutdown(ctx context.Context) {
	close(s.closed)
	_ = s.red.Close(ctx)
	_ = s.http.Shutdown(ctx)
	s.Command.Shutdown(ctx)
}

func (s *Server) Accept(conn *redcon.Conn) bool {
	utils.ZapLog.Debug("Accept", zap.String("remote", conn.RemoteAddr()))
	id, err := s.Command.GetCurrentID()
	if err != nil {
		id = oracle.EncodeTSO(time.Now().UnixMilli())
	}
	conn.ID = id
	return true
}

func (s *Server) Close(conn *redcon.Conn, err error) {
	utils.ZapLog.Debug("Close", zap.String("remote", conn.RemoteAddr()), zap.Error(err))
}
