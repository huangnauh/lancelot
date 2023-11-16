package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	rdb "github.com/hdt3213/rdb/parser"
	"go.uber.org/zap"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func (c *Command) SlaveOf(conn *redcon.Conn, cmd redcon.Command) error {
	if len(cmd.Args) != 3 {
		err := xerror.WrongArgsError(SLAVEOF_COMMAND)
		WriteConnError(conn, SLAVEOF_COMMAND, err)
		return err
	}

	txn, _ := c.getTransaction(conn)
	if txn != nil {
		WriteConnError(conn, SLAVEOF_COMMAND, xerror.ErrSlaveOfInsideMulti)
		return xerror.ErrSlaveOfInsideMulti
	}
	if strings.ToLower(string(cmd.Args[1])) == "no" &&
		strings.ToLower(string(cmd.Args[2])) == "one" {
		conn.WriteAny(OK)
		return nil
	}
	masterHost := string(cmd.Args[1])
	masterPort, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		WriteConnError(conn, WATCH_COMMAND, xerror.ErrNotInteger)
		return xerror.ErrNotInteger
	}
	cfg := c.GetConfig(conn.UserId)
	c.SetSlave(masterHost, cfg.Auth.Pass, cfg.Host, masterPort, cfg.RedisPort)
	go c.startSlave()
	conn.WriteAny(OK)
	return nil
}

type Slave struct {
	sync.Mutex
	userId            uint16
	configVersion     int32
	masterHost        string
	masterPort        int
	masterAuth        string
	slaveAnnounceIP   string
	slaveAnnouncePort int
	replId            string
	replOffset        int64
	lastRecvTime      time.Time
	ctx               context.Context
	cancel            context.CancelFunc
	conn              redis.Conn
}

func (c *Command) SetSlave(masterHost, masterAuth, slaveAnnounceIP string,
	masterPort, slaveAnnouncePort int) {
	c.slave.Lock()
	defer c.slave.Unlock()
	c.slave.configVersion += 1
	c.slave.masterHost = masterHost
	c.slave.masterPort = masterPort
	c.slave.masterAuth = masterAuth
	c.slave.slaveAnnouncePort = slaveAnnouncePort
	c.slave.slaveAnnounceIP = slaveAnnounceIP
	c.role = slaveRole
}

func (c *Command) startSlave() {
	ctx, cancel := context.WithCancel(context.Background())
	c.slave.Lock()
	c.slave.ctx = ctx
	c.slave.cancel = cancel
	configVersion := c.slave.configVersion
	c.slave.Unlock()

}

func (c *Command) stopSlave() {
	if c.slave.cancel != nil {
		c.slave.cancel()
	}
}

func (c *Command) slaveOfNone() {
	c.slave.Lock()
	defer c.slave.Unlock()
	c.slave.masterHost = ""
	c.slave.masterPort = 0
	c.slave.replId = ""
	c.slave.replOffset = -1
	c.role = masterRole
	c.slave.configVersion += 1
}

func (c *Command) setupSlave() {

}

func (s *Slave) loadRDB() {

}

func (c *Command) StartSlave(s *Slave) error {
	address := fmt.Sprintf("%s:%d", s.masterHost, s.masterPort)
	conn, err := redis.Dial("tcp", address, redis.DialPassword(s.masterAuth))
	if err != nil {
		return err
	}
	_, err = conn.Do("ping")
	if err != nil {
		return err
	}
	_, err = conn.Do("REPLCONF", "listening-port", s.slaveAnnouncePort)
	if err != nil {
		return err
	}
	if s.slaveAnnounceIP != "" {
		_, err = conn.Do("REPLCONF", "ip-address", s.slaveAnnounceIP)
		if err != nil {
			return err
		}
	}

	_, err = conn.Do("REPLCONF", "capa", "psync2")
	if err != nil {
		return err
	}
	s.conn = conn
	s.lastRecvTime = time.Now()

	replId := "?"
	var replOffset int64 = -1
	if s.replId != "" {
		replId = s.replId
		replOffset = s.replOffset
	}
	err = conn.Send("psync", replId, replOffset)
	if err != nil {
		return err
	}
	err = conn.Flush()
	if err != nil {
		return err
	}
	psync, err := conn.Receive()
	psyncStatus, ok := psync.(string)
	if !ok {
		return errors.New("illegal payload header not a status reply")
	}

	headers := strings.Split(psyncStatus, " ")
	if len(headers) != 3 && len(headers) != 2 {
		return errors.New("illegal payload header: " + psyncStatus)
	}
	utils.ZapLog.Info("receive psync header from master")

	var isFullReSync bool
	if headers[0] == "FULLRESYNC" {
		utils.ZapLog.Info("full re-sync with master")
		s.replId = headers[1]
		s.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
		if err != nil {
			return errors.New("get illegal repl offset: " + headers[2])
		}
		isFullReSync = true
	} else if headers[0] == "CONTINUE" {
		utils.ZapLog.Info("continue partial sync")
		s.replId = headers[1]
		isFullReSync = false
	} else {
		return errors.New("illegal psync resp: " + psyncStatus)
	}
	utils.ZapLog.Info("receive psync header from master",
		zap.String("repl id", s.replId), zap.Int64("repl offset", s.replOffset))
	if isFullReSync {
		rdbPayload, err := conn.Receive()
		if err != nil {
			return err
		}
		rbdReply, ok := rdbPayload.([]byte)
		if !ok {
			return fmt.Errorf("illegal payload header: %v", rdbPayload)
		}
		utils.ZapLog.Info("receive rdb from master", zap.Int("bytes", len(rbdReply)))
		dec := rdb.NewDecoder(bytes.NewReader(rbdReply))
		if err != nil {
			return err
		}
		failed := false
		err = dec.Parse(func(o rdb.RedisObject) bool {
			dbID := o.GetDBIndex()
			txn := c.client.NewTxn()
			err := txn.Begin()
			if err != nil {
				failed = true
				return false
			}
			defer txn.Rollback()
			c.BeginTxn(txn, s.userId, uint8(dbID))
			switch o.GetType() {
			case rdb.StringType:
				str := o.(*rdb.StringObject)
			case rdb.ListType:
				listObj := o.(*rdb.ListObject)
			case rdb.HashType:
				hashObj := o.(*rdb.HashObject)
			case rdb.SetType:
				setObj := o.(*rdb.SetObject)
			case rdb.ZSetType:
				zsetObj := o.(*rdb.ZSetObject)
			}
			return true
		})
		if err != nil {
			return err
		}
		if failed {
			//TODO
		}

		// err = rdbLoader.LoadRDB(rdbDec)
		// if err != nil {
		// 	return errors.New("dump rdb failed: " + err.Error())
		// }
	}
}
