package grpc

import (
	"context"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

type Client struct {
	conn    *grpc.ClientConn
	Client  lancepb.LanceClient
	closed  chan struct{}
	address string
}

func NewClient(ctx context.Context, address string, opts ...grpc.DialOption) (*Client, error) {
	utils.ZapLog.Debug("grpc.NewClient", zap.String("address", address))
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithInitialConnWindowSize(2*1024*1024),
		grpc.WithWriteBufferSize(1024*1024),
		grpc.WithReadBufferSize(1024*1024),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(50*1024*1024),
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	client := lancepb.NewLanceClient(conn)

	return &Client{conn: conn, Client: client, address: address, closed: make(chan struct{})}, nil
}

func (c *Client) String() string {
	return c.address
}

func (c *Client) waitConnReady() (err error) {
	if c.conn.GetState() == connectivity.Ready {
		return
	}
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for {
		s := c.conn.GetState()
		if s == connectivity.Ready {
			cancel()
			break
		}
		if !c.conn.WaitForStateChange(dialCtx, s) {
			cancel()
			err = dialCtx.Err()
			return
		}
	}
	return
}

func (c *Client) Close() error {
	utils.ZapLog.Debug("grpc.Client.Close", zap.String("address", c.address))
	close(c.closed)
	err := c.conn.Close()
	return err
}

func (c *Client) Publish(ctx context.Context, req *lancepb.PubRequest) (*lancepb.PubResponse, error) {
	var resp *lancepb.PubResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.Publish(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PSNumPat(ctx context.Context, req *lancepb.PSNumPatRequest) (*lancepb.PSNumPatResponse, error) {
	var resp *lancepb.PSNumPatResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PSNumPat(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PSChannels(ctx context.Context, req *lancepb.PSChannelsRequest) (*lancepb.PSChannelsResponse, error) {
	var resp *lancepb.PSChannelsResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PSChannels(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PSNumSub(ctx context.Context, req *lancepb.PSNumSubRequest) (*lancepb.PSNumSubResponse, error) {
	var resp *lancepb.PSNumSubResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PSNumSub(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PFAdd(ctx context.Context, req *lancepb.PFAddRequest) (*lancepb.PFResponse, error) {
	var resp *lancepb.PFResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PFAdd(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PFCount(ctx context.Context, req *lancepb.PFCountRequest) (*lancepb.PFResponse, error) {
	var resp *lancepb.PFResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PFCount(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}

func (c *Client) PFMerge(ctx context.Context, req *lancepb.PFMergeRequest) (*lancepb.PFResponse, error) {
	var resp *lancepb.PFResponse
	var err error
	for i := 0; i < 3; i++ {
		err = c.waitConnReady()
		if err == nil {
			resp, err = c.Client.PFMerge(ctx, req)
		}
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Millisecond * 10 * time.Duration(i+1))
	}
	return nil, err
}
