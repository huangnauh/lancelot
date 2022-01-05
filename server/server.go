package server

import (
	"context"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // pprof
	"strings"
	"sync"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/grpc"
	"gitlab.s.upyun.com/platform/lancelot/metric"
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
			command.WriteConnError(conn, comma, xerror.ErrAuthentication)
			return
		}
		conn.UserName = s.Command.Default.Name
		conn.UserId = s.Command.Default.ID
	}

	metric.Metric.InFlight.Inc()
	defer metric.Metric.InFlight.Dec()
	start := time.Now()

	var err error
	if handler, ok := s.Command.ConnHandle[comma]; ok {
		utils.ZapLog.Debug("ConnHandle", zap.String("remote", conn.RemoteAddr()),
			zap.ByteStrings("args", cmd.Args))
		err = handler.Func(conn, cmd)
	} else {
		err = s.Command.TxnHandler(conn, comma, cmd)
	}

	spent := time.Since(start)
	if s.cfg.Log.Enable {
		msg := "OK"
		if err != nil {
			msg = err.Error()
			if len(msg) > s.cfg.Log.LineLimit {
				msg = msg[:s.cfg.Log.LineLimit]
			}
		}
		log.Printf("%s, %s, %s, %s\n", conn.RemoteAddr(), cmd.All(s.cfg.Log.LineLimit), spent, msg)
	}
	metric.Metric.RequestDuration.WithLabelValues(comma).Observe(spent.Seconds())
	metric.Metric.RequestTotal.WithLabelValues(comma).Inc()
}

func NewServer(cfg *config.Config) *Server {
	s := &Server{
		http:   &http.Server{},
		closed: make(chan bool),
		cfg:    cfg,
	}
	s.rpc = grpc.NewGrpcServer(cfg)
	lancepb.RegisterLanceServer(s.rpc.GRPCServer, s.Command)
	s.red = redcon.NewServer("", s.ServeRESP, s.Accept, s.Close)
	s.Command = command.NewCommand(s.red)
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
	s.Command.Info.Health = false
	_ = s.red.Close(ctx)
	close(s.closed)
	_ = s.http.Shutdown(ctx)
	s.Command.Shutdown(ctx)
}

func (s *Server) Accept(conn *redcon.Conn) bool {
	return s.Command.Accept(conn)
}

func (s *Server) Close(conn *redcon.Conn, err error) {
	s.Command.Close(conn, err)
}
