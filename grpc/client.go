package grpc

import (
	"context"

	"google.golang.org/grpc"
)

type Client struct {
	Conn *grpc.ClientConn
}

func NewClient(ctx context.Context, address string, opts ...grpc.DialOption) (*Client, error) {
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithInitialConnWindowSize(2*1024*1024),
		grpc.WithWriteBufferSize(1024*1024),
		grpc.WithReadBufferSize(1024*1024),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)))
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{Conn: conn}, nil
}

func (c *Client) Close() error {
	err := c.Conn.Close()
	return err
}
