package command

import (
	"context"
	"fmt"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

func GetRpcAddress(leader string, rpcPort int) string {
	if strings.Contains(leader, ":") {
		return leader
	}
	return fmt.Sprintf("%s:%d", leader, rpcPort)
}

func (c *Command) Publish(ctx context.Context, req *lancepb.PubRequest) (*lancepb.PubResponse, error) {
	utils.ZapLog.Info("publish recv", zap.String("req", req.String()))
	messages := make([]*PubSubMessage, 0, len(req.Messages))
	for _, request := range req.Messages {
		messages = append(messages, &PubSubMessage{
			Channel: request.Channel,
			Message: request.Messages,
		})
	}
	count := c.psManager.LocalPublishMessages(messages)
	return &lancepb.PubResponse{
		Counts: count,
	}, nil
}

func (c *Command) PSChannels(ctx context.Context, req *lancepb.PSChannelsRequest) (*lancepb.PSChannelsResponse, error) {
	channels, err := c.psManager.LocalChannels(req.Pattern)
	if err != nil {
		return nil, err
	}
	return &lancepb.PSChannelsResponse{
		Channels: channels,
	}, nil
}

func (c *Command) PSNumSub(ctx context.Context, req *lancepb.PSNumSubRequest) (*lancepb.PSNumSubResponse, error) {
	counts := c.psManager.LocalNUMSUB(req.Channels)
	return &lancepb.PSNumSubResponse{
		Counts: counts,
	}, nil
}

func (c *Command) PSNumPat(ctx context.Context, req *lancepb.PSNumPatRequest) (*lancepb.PSNumPatResponse, error) {
	count := c.psManager.LocalNUMPAT()
	return &lancepb.PSNumPatResponse{
		Count: int64(count),
	}, nil
}

func (c *Command) PFAdd(ctx context.Context, req *lancepb.PFAddRequest) (*lancepb.PFResponse, error) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()
	c.BeginTxn(txn, uint16(req.Userdb.User), uint8(req.Userdb.Db))
	// count, err := c.pfAdd(txn, req.Key, req.Elements)
	// if err != nil {
	// 	return nil, err
	// }
	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return &lancepb.PFResponse{
		// Count: count,
	}, nil
}

func (c *Command) PFCount(ctx context.Context, req *lancepb.PFCountRequest) (*lancepb.PFResponse, error) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()
	c.BeginTxn(txn, uint16(req.Userdb.User), uint8(req.Userdb.Db))
	// count, err := c.pfCount(txn, req.Keys)
	// if err != nil {
	// 	return nil, err
	// }
	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return &lancepb.PFResponse{
		// Count: count,
	}, nil
}

func (c *Command) PFMerge(ctx context.Context, req *lancepb.PFMergeRequest) (*lancepb.PFResponse, error) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()
	c.BeginTxn(txn, uint16(req.Userdb.User), uint8(req.Userdb.Db))
	// _, err = c.pfMerge(txn, req.Destkey, req.Sourcekeys)
	// if err != nil {
	// 	return nil, err
	// }
	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return &lancepb.PFResponse{}, nil
}
