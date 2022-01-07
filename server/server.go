package server

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // pprof
	"os"
	"runtime"
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

var (
	dunno     = []byte("???")
	centerDot = []byte("·")
	dot       = []byte(".")
	slash     = []byte("/")
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

func stack(skip int) []byte {
	buf := new(bytes.Buffer) // the returned data
	// As we loop, we open files and read them. These variables record the currently
	// loaded file.
	var lines [][]byte
	var lastFile string
	for i := skip; ; i++ { // Skip the expected number of frames
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		// Print this much at least.  If we can't find the source, it won't show.
		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, line, pc)
		if file != lastFile {
			data, err := ioutil.ReadFile(file)
			if err != nil {
				continue
			}
			lines = bytes.Split(data, []byte{'\n'})
			lastFile = file
		}
		fmt.Fprintf(buf, "\t%s: %s\n", function(pc), source(lines, line))
	}
	return buf.Bytes()
}

// source returns a space-trimmed slice of the n'th line.
func source(lines [][]byte, n int) []byte {
	n-- // in stack trace, lines are 1-indexed but our array is 0-indexed
	if n < 0 || n >= len(lines) {
		return dunno
	}
	return bytes.TrimSpace(lines[n])
}

// function returns, if possible, the name of the function containing the PC.
func function(pc uintptr) []byte {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return dunno
	}
	name := []byte(fn.Name())
	// The name includes the path name to the package, which is unnecessary
	// since the file name is already included.  Plus, it has center dots.
	// That is, we see
	//	runtime/debug.*T·ptrmethod
	// and want
	//	*T.ptrmethod
	// Also the package path might contains dot (e.g. code.google.com/...),
	// so first eliminate the path prefix
	if lastSlash := bytes.LastIndex(name, slash); lastSlash >= 0 {
		name = name[lastSlash+1:]
	}
	if period := bytes.Index(name, dot); period >= 0 {
		name = name[period+1:]
	}
	name = bytes.Replace(name, centerDot, dot, -1)
	return name
}

func (s *Server) ServeRESP(conn *redcon.Conn, cmd redcon.Command) {
	comma := strings.ToLower(utils.B2S(cmd.Args[0]))
	defer func() {
		if err := recover(); err != nil {
			var brokenPipe bool
			if ne, ok := err.(*net.OpError); ok {
				if se, ok := ne.Err.(*os.SyscallError); ok {
					if strings.Contains(strings.ToLower(se.Error()), "broken pipe") || strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
						brokenPipe = true
					}
				}
			}

			stack := stack(3)
			if brokenPipe {
				log.Printf("%s\n", err)
			} else {
				log.Printf("[Recovery] panic:\n%s\n%s", err, stack)
				command.WriteConnError(conn, comma, xerror.ErrNotSupport)
			}
		}
	}()
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
	if s.cfg.Log.Enable && (err != nil || spent >= s.cfg.Log.SlowLog) {
		msg := "OK"
		if err != nil {
			msg = err.Error()
			if len(msg) > s.cfg.Log.LineLimit {
				msg = msg[:s.cfg.Log.LineLimit]
			}
		}
		log.Printf("%s, %d %s, %s, %s, %s\n", conn.RemoteAddr(), conn.UserId, conn.UserName,
			cmd.All(s.cfg.Log.ArgLimit, s.cfg.Log.LineLimit), spent, msg)
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
