package command

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/grpc"
	"gitlab.s.upyun.com/platform/lancelot/member"
	"gitlab.s.upyun.com/platform/lancelot/proto/lancepb"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	NormalChannel  = 0
	PatternChannel = 1

	PUBSUB_HELP = "PUBSUB HELP"
)

type PsConn struct {
	dconn    *redcon.DetachedConn
	messages chan []interface{}
	others   chan interface{}
	closed   chan struct{}
	channels [2]map[string]bool
}

func (p *PsConn) Close() {
	close(p.closed)
	close(p.messages)
	close(p.others)
}

func (p *PsConn) sendMessage() {
	for {
		select {
		case msg := <-p.messages:
			p.dconn.WriteAny(msg)
			p.dconn.Flush()
		case other := <-p.others:
			p.dconn.WriteAny(other)
			p.dconn.Flush()
			if other == OK {
				return
			}
		case <-p.closed:
			return
		}
	}
}

type PsManager struct {
	sync.RWMutex
	memberList *member.MemberList
	cfg        *config.PubSub
	hubs       [2]map[string]*Hub
}

func (p *PsManager) Channels() (int, int) {
	p.RLock()
	defer p.RUnlock()
	return len(p.hubs[0]), len(p.hubs[1])
}

type Hub struct {
	sync.RWMutex
	subscribes map[*PsConn]bool
}

func (h *Hub) GetSubscribes() []*PsConn {
	h.RLock()
	defer h.RUnlock()
	subscribes := make([]*PsConn, 0, len(h.subscribes))
	for sub := range h.subscribes {
		subscribes = append(subscribes, sub)
	}
	return subscribes
}

func (h *Hub) SetSubscribe(conn *PsConn) {
	h.Lock()
	defer h.Unlock()
	h.subscribes[conn] = true
}

func (p *PsManager) Clients() []*grpc.Client {
	if p.memberList == nil {
		return nil
	}
	return p.memberList.GetClients()
}

func (p *PsManager) WaitAlive() error {
	if p.memberList == nil {
		return nil
	}
	return p.memberList.WaitAlive()
}

func (p *PsManager) handleHub(h *Hub, channel string, messages [][]byte, pattern string) int {
	count := 0
	subs := h.GetSubscribes()
	utils.ZapLog.Debug("[Hub] handle", zap.String("channel", channel), zap.Int("subscribes", len(subs)))

SUBSLOOP:
	for _, sub := range subs {
		for _, msg := range messages {

			var message []interface{}
			if pattern == "" {
				message = []interface{}{"message", channel, msg}
			} else {
				message = []interface{}{"pmessage", pattern, channel, msg}
			}
			select {
			case sub.messages <- message:
				count++
			case <-sub.closed:
				continue SUBSLOOP
			default:
				// close slow subscriber
				p.Close(sub)
				continue SUBSLOOP
			}
		}
	}
	return count
}

func (p *PsManager) Close(ps *PsConn) {
	for i := 0; i < 2; i++ {
		for channel := range ps.channels[i] {
			p.unsubscribeChannel(ps, channel, i)
		}
	}
	ps.Close()
}

func NewPsManager(cfg *config.PubSub, memberList *member.MemberList) *PsManager {
	psManager := &PsManager{
		hubs: [2]map[string]*Hub{
			0: make(map[string]*Hub),
			1: make(map[string]*Hub),
		},
		memberList: memberList,
		cfg:        cfg,
	}
	return psManager
}

func (p *PsManager) GetChannelHub(channel string) (*Hub, bool) {
	p.RLock()
	defer p.RUnlock()
	hub, ok := p.hubs[0][channel]
	return hub, ok
}

func (p *PsManager) GetPatternHubs() map[string]*Hub {
	p.RLock()
	defer p.RUnlock()
	hubs := make(map[string]*Hub)
	for pattern, hub := range p.hubs[1] {
		hubs[pattern] = hub
	}
	return hubs
}

func (p *PsManager) LocalNUMPAT() int64 {
	p.RLock()
	defer p.RUnlock()
	return int64(len(p.hubs[1]))
}

func (p *PsManager) LocalNUMSUB(channels []string) []int64 {
	count := make([]int64, len(channels))
	p.RLock()
	defer p.RUnlock()
	for i, channel := range channels {
		if hub, ok := p.hubs[0][channel]; ok {
			count[i] = int64(len(hub.GetSubscribes()))
		}
	}
	return count
}

func (p *PsManager) LocalChannels(pattern string) ([]string, error) {
	p.RLock()
	channels := make([]string, 0, len(p.hubs[0]))
	for channel := range p.hubs[0] {
		channels = append(channels, channel)
	}
	p.RUnlock()
	if pattern == "*" {
		return channels, nil
	}
	matches := make([]string, 0)
	for _, channel := range channels {
		match, err := glob.Match(pattern, channel)
		if err != nil {
			utils.ZapLog.Error("[Channels] glob.Match error", zap.String("pattern", pattern), zap.Error(err))
			return nil, err
		}
		if match {
			matches = append(matches, channel)
		}
	}
	return matches, nil
}

type PubSubMessage struct {
	Channel string
	Message [][]byte
}

// (pubsub) PUBLISH channel message
func (c *Command) PublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetError(xerror.WrongArgsError(PUBLISH_COMMAND))
	}

	channel := string(args[0])
	message := &PubSubMessage{Channel: channel, Message: args[1:]}
	if txn.Multi {
		return message
	} else {
		return c.psManager.PublishMessages([]*PubSubMessage{message})[0]
	}
}

func (p *PsManager) LocalPublishMessages(messages []*PubSubMessage) []int64 {
	count := make([]int64, len(messages))
	for i, message := range messages {
		c := p.publish(message.Channel, message.Message)
		count[i] = int64(c)
	}
	return count
}

func (p *PsManager) PublishMessages(messages []*PubSubMessage) []redcon.SimpleInt {
	clients := p.Clients()
	wg := &sync.WaitGroup{}
	wg.Add(len(clients) + 1)
	count := make([][]int64, len(clients)+1)

	go func(i int) {
		count[i] = p.LocalPublishMessages(messages)
		wg.Done()
	}(len(clients))

	if len(clients) > 0 {
		req := &lancepb.PubRequest{
			Messages: make([]*lancepb.PubRequestMessage, len(messages)),
		}
		for i, message := range messages {
			req.Messages[i] = &lancepb.PubRequestMessage{
				Channel:  message.Channel,
				Messages: message.Message,
			}
		}

		for i, client := range clients {
			go func(i int, client *grpc.Client) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := client.Publish(ctx, req)
				cancel()
				if err != nil {
					utils.ZapLog.Error("publish error", zap.Error(err), zap.String("client", client.String()))
					return
				}
				count[i] = resp.Counts

			}(i, client)
		}
	}
	wg.Wait()
	counts := make([]redcon.SimpleInt, len(messages))
	for _, c := range count {
		for i := 0; i < len(c); i++ {
			counts[i] += redcon.SimpleInt(c[i])
		}
	}
	return counts
}

func (p *PsManager) publish(channel string, messages [][]byte) int {
	// local
	count := 0
	hub, ok := p.GetChannelHub(channel)
	if ok {
		count += p.handleHub(hub, channel, messages, "")
	}

	hubs := p.GetPatternHubs()
	for pattern, hub := range hubs {
		match, _ := glob.Match(pattern, channel)
		if match {
			count += p.handleHub(hub, channel, messages, pattern)
		}
	}
	return count
}

// UNSUBSCRIBE channel [channel ...]
func (c *Command) UnsubscribeHandle(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		conn.WriteArray(3)
		conn.WriteBulkString("unsubscribe")
		conn.WriteBulkString("all")
		conn.WriteInt(0)
		return
	}
	for _, resp := range cmd.Args[1:] {
		conn.WriteArray(3)
		conn.WriteBulkString("unsubscribe")
		conn.WriteBulk(resp)
		conn.WriteInt(0)
	}
}

// PUNSUBSCRIBE channel [channel ...]
func (c *Command) PUnsubscribeHandle(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		conn.WriteArray(3)
		conn.WriteBulkString("punsubscribe")
		conn.WriteBulkString("all")
		conn.WriteInt(0)
		return
	}
	for _, resp := range cmd.Args[1:] {
		conn.WriteArray(3)
		conn.WriteBulkString("punsubscribe")
		conn.WriteBulk(resp)
		conn.WriteInt(0)
	}
}

// SUBSCRIBE channel [channel ...]
func (c *Command) SubscribeHandle(conn *redcon.Conn, cmd redcon.Command) {
	c.subscribe(conn, cmd, NormalChannel)
}

// PSUBSCRIBE channel [channel ...]
func (c *Command) PSubscribeHandle(conn *redcon.Conn, cmd redcon.Command) {
	c.subscribe(conn, cmd, PatternChannel)
}

func (c *Command) subscribe(conn *redcon.Conn, cmd redcon.Command, channelType int) {
	if len(cmd.Args) < 2 {
		conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
		return
	}

	err := c.psManager.WaitAlive()
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ps := &PsConn{
		dconn: conn.Detach(),
		channels: [2]map[string]bool{
			NormalChannel:  make(map[string]bool),
			PatternChannel: make(map[string]bool),
		},
		messages: make(chan []interface{}, c.cfg.PubSub.MaxSlowMessagePerSubscribe),
		others:   make(chan interface{}, 2),
		closed:   make(chan struct{}),
	}
	go ps.sendMessage()
	c.psManager.subscribe(ps, cmd.Args[1:], channelType)
	go c.psManager.runDetach(ps)
}

func (p *PsManager) unsubscribeChannel(ps *PsConn, channel string, channelType int) {
	p.Lock()
	hub, ok := p.hubs[channelType][channel]
	p.Unlock()
	if !ok {
		return
	}

	hub.Lock()
	delete(hub.subscribes, ps)
	if len(hub.subscribes) == 0 {
		p.Lock()
		delete(p.hubs[channelType], channel)
		p.Unlock()
	}
	hub.Unlock()
	delete(ps.channels[channelType], channel)
}

func (p *PsManager) unsubscribe(ps *PsConn, args [][]byte, channelType int) {
	command := UNSUBSCRIBE_COMMAND
	if channelType == PatternChannel {
		command = PUNSUBSCRIBE_COMMAND
	}

	if len(args) == 0 {
		for channel := range ps.channels[channelType] {
			p.unsubscribeChannel(ps, channel, channelType)
		}
		ps.messages <- []interface{}{command, "all", redcon.SimpleInt(0)}
		return
	}

	for _, arg := range args {
		channel := utils.B2S(arg)
		p.unsubscribeChannel(ps, channel, channelType)
		ps.messages <- []interface{}{command, channel, redcon.SimpleInt(len(ps.channels[channelType]))}
	}
}

func (p *PsManager) subscribe(ps *PsConn, args [][]byte, channelType int) {
	for _, arg := range args {
		channel := utils.B2S(arg)
		p.Lock()
		hub, ok := p.hubs[channelType][channel]
		if !ok {
			hub = &Hub{
				subscribes: map[*PsConn]bool{ps: true},
			}
			p.hubs[channelType][channel] = hub
		}
		p.Unlock()
		if ok {
			hub.Lock()
			hub.subscribes[ps] = true
			hub.Unlock()
		}
		ps.channels[channelType][channel] = true
		command := SUBSCRIBE_COMMAND
		if channelType == PatternChannel {
			command = PSUBSCRIBE_COMMAND
		}
		ps.messages <- []interface{}{command, channel, redcon.SimpleInt(len(ps.channels[channelType]))}
	}
}

func (p *PsManager) runDetach(sconn *PsConn) {
	defer func() {
		select {
		case <-sconn.closed:
			return
		default:
			p.Close(sconn)
		}
	}()

	utils.ZapLog.Debug("detached", zap.String("remote", sconn.dconn.RemoteAddr()))
	for {
		cmd, err := sconn.dconn.ReadCommand()
		if err != nil {
			utils.ZapLog.Debug("[PubSub] ReadCommand", zap.Error(err))
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		utils.ZapLog.Debug("detached", zap.String("remote", sconn.dconn.RemoteAddr()),
			zap.ByteStrings("args", cmd.Args))
		comma := string(cmd.Args[0])
		switch strings.ToLower(comma) {
		case "subscribe":
			if len(cmd.Args) < 2 {
				sconn.others <- xerror.WrongArgsError(SUBSCRIBE_COMMAND)
				continue
			}
			p.subscribe(sconn, cmd.Args[1:], NormalChannel)
		case "psubscribe":
			if len(cmd.Args) < 2 {
				sconn.others <- xerror.WrongArgsError(PSUBSCRIBE_COMMAND)
				continue
			}
			p.subscribe(sconn, cmd.Args[1:], PatternChannel)
			if err != nil {
				sconn.others <- err
				continue
			}
		case "unsubscribe":
			p.unsubscribe(sconn, cmd.Args[1:], NormalChannel)
		case "punsubscribe":
			p.unsubscribe(sconn, cmd.Args[1:], PatternChannel)
		case "quit":
			sconn.others <- OK
			return
		case "ping":
			var msg string
			switch len(cmd.Args) {
			case 1:
			case 2:
				msg = string(cmd.Args[1])
			default:
				sconn.others <- xerror.WrongArgsError(PING_COMMAND)
				continue
			}
			sconn.messages <- []interface{}{"pong", msg}
		default:
			sconn.others <- xerror.InvalidCommandError(comma)
		}
	}
}

func (c *Command) PubSubHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(PUBSUB_COMMAND))
	}
	subcommand := strings.ToLower(utils.B2S(args[0]))
	var err error
	var ret interface{}
	switch subcommand {
	case CHANNELS_COMMAND:
		ret, err = c.psManager.pubSubChannels(txn, args[1:])
	case NUMSUB_COMMAND:
		numsubs, err := c.psManager.pubSubNumsub(txn, args[1:])
		if err != nil {
			return txn.SetError(err)
		}
		ret := make([]interface{}, 2*len(numsubs))
		for i, numsub := range numsubs {
			ret[2*i] = numsub.Channel
			ret[2*i+1] = redcon.SimpleInt(numsub.Count)
		}
		return ret
	case NUMPAT_COMMAND:
		ret, err = c.psManager.pubSubNumpat(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(PUBSUB_COMMAND, PUBSUB_HELP)
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// PUBSUB NUMPAT
func (p *PsManager) pubSubNumpat(txn *store.Txn, args [][]byte) (redcon.SimpleInt, error) {
	if len(args) != 0 {
		return 0, xerror.WrongSubArgsError(NUMPAT_COMMAND, PUBSUB_HELP)
	}
	clients := p.Clients()
	count := p.LocalNUMPAT()
	if len(clients) == 0 {
		return redcon.SimpleInt(count), nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(clients))

	req := &lancepb.PSNumPatRequest{}
	for _, client := range clients {
		go func(client *grpc.Client) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := client.PSNumPat(ctx, req)
			cancel()
			if err != nil {
				utils.ZapLog.Error("[PubSub] PSNumPat", zap.Error(err))
				return
			}
			atomic.AddInt64(&count, resp.Count)
		}(client)
	}
	wg.Wait()
	return redcon.SimpleInt(count), nil
}

// PUBSUB CHANNELS [pattern]
func (p *PsManager) pubSubChannels(txn *store.Txn, args [][]byte) ([]string, error) {
	if len(args) > 1 {
		return nil, xerror.WrongSubArgsError(CHANNELS_COMMAND, PUBSUB_HELP)
	}
	pattern := "*"
	if len(args) == 1 {
		pattern = string(args[0])
	}
	channels, err := p.LocalChannels(pattern)
	if err != nil {
		return nil, err
	}
	clients := p.Clients()
	if len(clients) == 0 {
		return channels, nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(clients))
	req := &lancepb.PSChannelsRequest{Pattern: pattern}
	remoteChannels := make([][]string, len(clients))
	for i, client := range clients {
		go func(i int, client *grpc.Client) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := client.PSChannels(ctx, req)
			cancel()
			if err != nil {
				utils.ZapLog.Error("[PubSub] PSNumPat", zap.Error(err))
				return
			}
			remoteChannels[i] = resp.Channels
		}(i, client)
	}
	wg.Wait()
	for _, cs := range remoteChannels {
		channels = append(channels, cs...)
	}
	return channels, nil
}

type NumSub struct {
	Channel string
	Count   int64
}

// PUBSUB NUMSUB [channel-1 ... channel-N]
func (p *PsManager) pubSubNumsub(txn *store.Txn, args [][]byte) ([]*NumSub, error) {
	if len(args) < 1 {
		return []*NumSub{}, nil
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = utils.B2S(arg)
	}
	clients := p.Clients()
	allCounts := make([][]int64, len(clients)+1)
	allCounts[len(clients)] = p.LocalNUMSUB(channels)
	if len(clients) > 0 {
		wg := &sync.WaitGroup{}
		wg.Add(len(clients))
		req := &lancepb.PSNumSubRequest{Channels: channels}

		for i, client := range clients {
			go func(i int, client *grpc.Client) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := client.PSNumSub(ctx, req)
				cancel()
				if err != nil {
					utils.ZapLog.Error("[PubSub] PSNumsub", zap.Error(err))
					return
				}
				allCounts[i] = resp.Counts
			}(i, client)
		}
	}
	counts := make([]*NumSub, len(channels))
	for k, c := range allCounts {
		for i := 0; i < len(c); i++ {
			if k == 0 {
				counts[i] = &NumSub{Channel: channels[i], Count: c[i]}
			} else {
				counts[i].Count += c[i]
			}
		}
	}
	return counts, nil
}
