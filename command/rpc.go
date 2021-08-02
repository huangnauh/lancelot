package command

import (
	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

func (c *Command) Publush(ss lancepb.Lance_PublushServer) error {
	for {
		reqs, err := ss.Recv()
		if err != nil {
			utils.ZapLog.Error("publish recv", zap.Error(err))
			return err
		}

		// resps := make([]*lancepb.PubResponse, len(reqs.Message))
		for _, req := range reqs.Message {
			utils.ZapLog.Info("publish recv", zap.String("req", req.String()))
		}
	}
}
func (c *Command) Subscribe(ss lancepb.Lance_SubscribeServer) error {
	return nil
}
