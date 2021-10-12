package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	newSessionRetryCnt      = 3
	managerSessionTTL       = 60

	ManagerKey = "/lancelot/manager"
)

type Manager struct {
	id           string
	key          string
	leader       string
	leaderChan   chan bool
	resumeLeader bool
	ctx          context.Context
	cancel       context.CancelFunc
	etcdCli      *clientv3.Client
	wg           sync.WaitGroup
}

func NewManager(etcdCli *clientv3.Client, id string) *Manager {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Manager{
		etcdCli:      etcdCli,
		ctx:          ctx,
		cancel:       cancelFunc,
		id:           id,
		key:          ManagerKey,
		wg:           sync.WaitGroup{},
		resumeLeader: true,
		leaderChan:   make(chan bool, 1),
	}
}

func (m *Manager) Cancel() {
	m.cancel()
	m.wg.Wait()
	close(m.leaderChan)
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, grpc.ErrClientConnClosing) {
		return err
	}
	return nil
}

func (m *Manager) NewSession(id int64) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < newSessionRetryCnt; i++ {
		if err = contextDone(m.ctx, err); err != nil {
			return etcdSession, err
		}
		etcdSession, err = concurrency.NewSession(m.etcdCli,
			concurrency.WithTTL(managerSessionTTL),
			concurrency.WithContext(m.ctx),
			concurrency.WithLease(clientv3.LeaseID(id)))
		if err == nil {
			break
		}

		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	if err != nil {
		utils.ZapLog.Error("create etcd session failed", zap.Error(err))
	}
	return etcdSession, err
}

func (m *Manager) revokeSession(leaseID clientv3.LeaseID) {
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(managerSessionTTL)*time.Second)
	defer cancel()
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	if err != nil {
		utils.ZapLog.Error("revoke session", zap.Error(err))
	} else {
		utils.ZapLog.Info("revoke session")
	}
}

func (m *Manager) setLeader(id string) {
	if id == "" {
		utils.ZapLog.Info("clear leader")
	} else {
		utils.ZapLog.Info("set leader", zap.String("id", id))
	}

	m.leader = id
	select {
	case m.leaderChan <- true:
	default:
	}
}

func (m *Manager) GetLeader(ctx context.Context) string {
	if m.etcdCli == nil {
		return m.id
	}

	if m.leader != "" {
		return m.leader
	}

	for {
		select {
		case <-m.leaderChan:
			if m.leader != "" {
				return m.leader
			}
		case <-ctx.Done():
			return ""
		case <-m.ctx.Done():
			return ""
		}
	}
}

func (m *Manager) RunElection() error {
	if m.etcdCli == nil {
		return nil
	}
	var observe <-chan clientv3.GetResponse
	var node *clientv3.GetResponse
	var errChan chan error
	var err error

	session, err := m.NewSession(0)
	if err != nil {
		return err
	}
	election := concurrency.NewElection(session, m.key)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			// Discover who if any, is leader of this election
			if node, err = election.Leader(m.ctx); err != nil {
				if err != concurrency.ErrElectionNoLeader {
					utils.ZapLog.Error("while determining election leader", zap.Error(err))
					goto reconnect
				}
				m.setLeader("")
			} else {
				// If we are resuming an election from which we previously had leadership we
				// have 2 options
				// 1. Resume the leadership if the lease has not expired. This is a race as the
				//    lease could expire in between the `Leader()` call and when we resume
				//    observing changes to the election. If this happens we should detect the
				//    session has expired during the observation loop.
				// 2. Resign the leadership immediately to allow a new leader to be chosen.
				//    This option will almost always result in transfer of leadership.
				leader := string(node.Kvs[0].Value)
				if leader == m.id {
					// If we want to resume leadership
					if m.resumeLeader {
						// Recreate our session with the old lease id
						session, err = m.NewSession(node.Kvs[0].Lease)
						if err != nil {
							utils.ZapLog.Error("while re-establishing session with lease", zap.Error(err))
							goto reconnect
						}
						election = concurrency.ResumeElection(session, m.key,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)

						// Because Campaign() only returns if the election entry doesn't exist
						// we must skip the campaign call and go directly to observe when resuming
						goto observe
					} else {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(m.ctx, time.Duration(managerSessionTTL)*time.Second)
						election := concurrency.ResumeElection(session, m.key,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)
						err = election.Resign(ctx)
						cancel()
						if err != nil {
							utils.ZapLog.Error("while resigning leadership after reconnect", zap.Error(err))
							goto reconnect
						}
					}
				}
				m.setLeader(leader)
			}
			// Reset leadership if we had it previously

			// Attempt to become leader
			errChan = make(chan error)
			go func() {
				// Make this a non blocking call so we can check for session close
				errChan <- election.Campaign(m.ctx, m.id)
			}()

			select {
			case err = <-errChan:
				if err != nil {
					session.Close()
					// NOTE: Campaign currently does not return an error if session expires
					utils.ZapLog.Error("while campaigning for leader", zap.Error(err))
					goto reconnect
				}
			case <-m.ctx.Done():
				session.Close()
				return
			case <-session.Done():
				goto reconnect
			}

		observe:
			// If Campaign() returned without error, we are leader
			m.setLeader(m.id)

			// Observe changes to leadership
			observe = election.Observe(m.ctx)
			for {
				select {
				case resp, ok := <-observe:
					if !ok {
						// NOTE: Observe will not close if the session expires, we must
						// watch for session.Done()
						session.Close()
						goto reconnect
					}
					leader := string(resp.Kvs[0].Value)
					m.setLeader(leader)
				case <-m.ctx.Done():
					if m.leader == m.id {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(managerSessionTTL)*time.Second)
						if err = election.Resign(ctx); err != nil {
							utils.ZapLog.Error("while resigning leadership during shutdown", zap.Error(err))
						}
						cancel()
					}
					session.Close()
					return
				case <-session.Done():
					goto reconnect
				}
			}

		reconnect:
			m.setLeader("")

			select {
			case <-m.ctx.Done():
				return
			default:
			}

			for {
				session, err = m.NewSession(0)
				if err != nil {
					utils.ZapLog.Error("while creating new session", zap.Error(err))
					tick := time.NewTicker(newSessionRetryInterval * newSessionRetryCnt)
					select {
					case <-m.ctx.Done():
						tick.Stop()
						return
					case <-tick.C:
						tick.Stop()
					}
					continue
				}
				break
			}
		}
	}()
	return nil
}
