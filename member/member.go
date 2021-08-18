package member

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"gitlab.s.upyun.com/platform/lancelot/grpc"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	MEMBERS         = "/lancelot/members/"
	DefaultLeaseTTL = time.Second * 5
)

type Member struct {
	Lease   int64
	Address string
	Version int64
	Client  *grpc.Client
}

type MemberList struct {
	Address       string
	Alive         bool
	sessionChange chan bool
	session       *concurrency.Session
	etcdCli       *clientv3.Client
	Members       map[string]*Member
	sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMemberList(etcdCli *clientv3.Client, addr string) *MemberList {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemberList{
		Address:       addr,
		etcdCli:       etcdCli,
		Members:       make(map[string]*Member),
		sessionChange: make(chan bool),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (m *MemberList) Start() error {
	err := m.NewSession()
	if err != nil {
		return err
	}

	go m.runWatch()
	return nil
}

func (m *MemberList) GetClients() []*grpc.Client {
	m.RLock()
	defer m.RUnlock()
	clients := make([]*grpc.Client, 0, len(m.Members))
	for _, member := range m.Members {
		clients = append(clients, member.Client)
	}
	return clients
}

func (m *MemberList) GetMemberKey() string {
	return fmt.Sprintf("%s%d", MEMBERS, m.session.Lease())
}

func (m *MemberList) Close() {
	m.cancel()
	m.Alive = false
	m.session.Close()
	m.Lock()
	defer m.Unlock()
	for _, member := range m.Members {
		member.Client.Close()
	}
}

func (m *MemberList) GetSession() error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	default:
	}

	m.Alive = false
	err := m.NewSession()
	if err == nil {
		return nil
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = time.Millisecond * 100
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = 0
	bo.Reset()
	tick := time.NewTicker(bo.NextBackOff())
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-tick.C:
			err = m.NewSession()
			if err == nil {
				return nil
			}
			tick.Reset(bo.NextBackOff())
		}
	}
}

func (m *MemberList) NewSession() error {
	etcdSession, err := concurrency.NewSession(m.etcdCli, concurrency.WithTTL(60))
	if err != nil {
		utils.ZapLog.Error("create session error", zap.Error(err))
		return err
	}
	m.session = etcdSession

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	_, err = m.etcdCli.Put(ctx, m.GetMemberKey(), m.Address, clientv3.WithLease(m.session.Lease()))
	cancel()
	if err != nil {
		utils.ZapLog.Error("create member error", zap.Error(err))
		return err
	}
	err = m.GetALL()
	if err != nil {
		return err
	}
	m.Alive = true
	select {
	case m.sessionChange <- true:
	default:
	}
	return nil
}

func (m *MemberList) WaitAlive() error {
	if m.Alive {
		return nil
	}

	utils.ZapLog.Debug("[member] wait alive")
	tick := time.NewTicker(10 * time.Second)
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-tick.C:
		return xerror.TimeOut
	case <-m.sessionChange:
		return nil
	}
}

func (m *MemberList) SetKv(kv *mvccpb.KeyValue, version int64, deleted bool) string {
	key := strings.TrimPrefix(utils.B2S(kv.Key), MEMBERS)
	id, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		utils.ZapLog.Error("etcd watch event key is not a number", zap.String("key", key))
		return ""
	}
	addr := string(kv.Value)
	if addr == m.Address {
		return addr
	}

	utils.ZapLog.Debug("[member] set kv", zap.Int64("lease", id), zap.String("address", addr))

	m.Lock()
	member, ok := m.Members[addr]
	if ok && member.Version > version {
		m.Unlock()
		return addr
	}
	if deleted {
		delete(m.Members, addr)
		m.Unlock()
		if member != nil {
			member.Client.Close()
		}
		return addr
	}

	if ok {
		member.Version = version
		member.Lease = id
		m.Unlock()
		return addr
	}

	client, err := grpc.NewClient(m.ctx, addr)
	if err != nil {
		// unexpected error
		m.Unlock()
		utils.ZapLog.Error("create client error", zap.Error(err))
		return addr
	}
	m.Members[addr] = &Member{
		Lease:   id,
		Address: addr,
		Version: version,
		Client:  client,
	}
	m.Unlock()
	return addr
}

func (m *MemberList) GetALL() error {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	resp, err := m.etcdCli.Get(ctx, MEMBERS, clientv3.WithPrefix())
	cancel()
	if err != nil {
		utils.ZapLog.Error("get all members error", zap.Error(err))
		return err
	}

	addrs := make(map[string]bool)
	for _, ev := range resp.Kvs {
		addr := m.SetKv(ev, ev.ModRevision, false)
		if addr != "" {
			addrs[addr] = true
		}
	}

	membs := make([]*Member, 0)
	m.Lock()
	for addr, member := range m.Members {
		if _, ok := addrs[addr]; !ok {
			delete(m.Members, addr)
			membs = append(membs, member)
		}
	}
	m.Unlock()

	for _, member := range membs {
		member.Client.Close()
	}

	return nil
}

func (m *MemberList) runWatch() {
	utils.ZapLog.Info("start member list watch", zap.String("watch", MEMBERS))
	watch := m.etcdCli.Watch(m.ctx, MEMBERS, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.session.Done():
			utils.ZapLog.Info("session expired")
			err := m.GetSession()
			if err != nil {
				return
			}
		case resp, ok := <-watch:
			if !ok || resp.Canceled {
				utils.ZapLog.Info("watch canceled")
				err := m.GetSession()
				if err != nil {
					return
				}
			}

			for _, ev := range resp.Events {
				kv := ev.Kv
				version := ev.Kv.ModRevision
				deleted := ev.Type == mvccpb.DELETE
				utils.ZapLog.Debug("watch event",
					zap.String("event", ev.Type.String()),
					zap.ByteString("key", kv.Key),
					zap.Int64("version", version),
				)
				if deleted {
					utils.ZapLog.Info("etcd watch event is delete")
					kv = ev.PrevKv
				}
				if kv == nil {
					utils.ZapLog.Info("etcd watch event is nil")
					continue
				}
				m.SetKv(kv, version, deleted)
			}
		}
	}
}
