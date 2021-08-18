package command

import (
	"context"

	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

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
