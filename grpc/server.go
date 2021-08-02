package grpc

import (
	"context"
	"fmt"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"google.golang.org/grpc"
)

type Server struct {
	GRPCServer *grpc.Server
	conf       *config.Rpc
	address    string
}

func NewGrpcServer(conf *config.Config) *Server {
	sopts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(conf.Rpc.MaxStream),
		grpc.InitialWindowSize(2 * 1024 * 1024),
		grpc.InitialConnWindowSize(2 * 1024 * 1024),
		grpc.MaxRecvMsgSize(conf.Rpc.MaxMsgSize),
		grpc.MaxSendMsgSize(conf.Rpc.MaxMsgSize),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	}

	grpcServer := grpc.NewServer(sopts...)

	grpc_prometheus.Register(grpcServer)

	return &Server{
		GRPCServer: grpcServer,
		conf:       &conf.Rpc,
		address:    fmt.Sprintf("%s:%d", conf.Host, conf.RpcPort),
	}
}

func (s *Server) Close(ctx context.Context) {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		s.GRPCServer.GracefulStop()
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		s.GRPCServer.Stop()
		<-ch
	}
}
