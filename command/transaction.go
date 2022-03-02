package command

import (
	"strconv"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/metric"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func (c *Command) checkSingle(conn *redcon.Conn, unwatch bool) (*store.Txn, bool) {
	connTxn := conn.Transaction()
	var txn *store.Txn
	if connTxn != nil {
		var ok bool
		txn, ok = connTxn.(*store.Txn)
		if ok {
			if txn.Multi {
				return txn, false
			} else {
				// after WATCH command but before MULTI command
				if unwatch {
					txn.Watch = false
					txn.Reset()
				}
			}
		}

	}
	newTxn := c.createTransaction(conn)
	return newTxn, true
}

func (c *Command) SingleHandler(conn *redcon.Conn, txn *store.Txn, txnHandle TxnHandle, comma string, args [][]byte) error {
	err := txn.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	resp := txnHandle(txn, args)
	utils.ZapLog.Debug("SingleHandler", zap.Any("resp", resp), zap.Error(txn.Err))
	if txn.Err != nil {
		err, ok := resp.(error)
		if ok {
			WriteConnError(conn, comma, err)
		} else {
			txn.WriteAny(resp)
		}
		return nil
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	txn.WriteAny(resp)
	return nil
}

func (c *Command) TxnHandler(conn *redcon.Conn, comma string, cmd redcon.Command) error {
	handler, ok := c.TxnHandle[comma]
	if !ok {
		txn, exist := c.getTransaction(conn)
		if exist && txn != nil && txn.Multi {
			txn.PendingErr = true
		}
		err := xerror.UnknownCommandError(comma)
		WriteConnError(conn, DISCARD_COMMAND, err)
		return err
	}
	txnHandle := handler.Func
	args := cmd.Args[1:]
	txn, single := c.checkSingle(conn, comma == UNWATCH_COMMAND)
	utils.ZapLog.Debug("TxnHandler", zap.String("remote", conn.RemoteAddr()),
		zap.Uint16("user-id", conn.UserId), zap.Uint8("db", conn.DBId),
		zap.ByteStrings("args", cmd.Args), zap.Bool("single", single))
	if single {
		var err error
		for i := 0; i < 3; i++ {
			err = c.SingleHandler(conn, txn, txnHandle, comma, args)
			if err == nil {
				return txn.Err
			}
			utils.ZapLog.Info("Retry SingleHandler", zap.String("cmd", cmd.String()), zap.Int("i", i))
			time.Sleep(time.Millisecond * time.Duration(i+1))
		}
		WriteConnError(conn, comma, err)
		return err
	}

	txn.PendingReq = append(txn.PendingReq, cmd)
	conn.WriteAny(Queued)

	// if txn.Exec {
	// 	if !txn.HasTransaction() {
	// 		txn.Err = xerror.InvalidTxn
	// 		WriteConnError(conn, xerror.InvalidTxn)
	// 		return
	// 	}
	// 	resp := txnHandle(txn, args)
	// 	if txn.Err == nil {
	// 		txn.PendingResp = append(txn.PendingResp, resp)
	// 	}
	// } else {

	// }
	return nil
}

func (c *Command) quit(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("quit", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	conn.WriteAny(OK)
	return conn.Close()
}

func (c *Command) command(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("quit", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	conn.WriteRaw(COMMANDS)
	return nil
}

func (c *Command) discard(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("discard", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		err := xerror.WrongArgsError(string(cmd.Args[0]))
		WriteConnError(conn, DISCARD_COMMAND, err)
		return err
	}
	txn, _ := c.getTransaction(conn)
	if txn == nil {
		WriteConnError(conn, DISCARD_COMMAND, xerror.ErrDISCARDErr)
		return xerror.ErrDISCARDErr
	}
	defer conn.SetTransaction(nil)
	if !txn.Multi {
		WriteConnError(conn, DISCARD_COMMAND, xerror.ErrDISCARDErr)
		return xerror.ErrDISCARDErr
	}
	txn.Rollback()
	conn.WriteAny(OK)
	return nil
}

func (c *Command) exec(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("Exec", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		err := xerror.WrongArgsError(string(cmd.Args[0]))
		WriteConnError(conn, EXEC_COMMAND, err)
		return err
	}

	defer conn.SetTransaction(nil)
	txn, exist := c.getTransaction(conn)
	if !exist || !txn.Multi {
		WriteConnError(conn, EXEC_COMMAND, xerror.ErrEXECErr)
		return xerror.ErrEXECErr
	}
	if txn.PendingErr {
		WriteConnError(conn, EXEC_COMMAND, xerror.ErrTransactionDiscarded)
		return xerror.ErrTransactionDiscarded
	}

	// not watch
	if !txn.HasTransaction() {
		err := txn.Begin()
		if err != nil {
			WriteConnError(conn, EXEC_COMMAND, err)
			return err
		}
	}

	// pennding
	txn.Exec = true
	ret := make([]interface{}, len(txn.PendingReq))
	var pubindex []int
	var pubMessage []*PubSubMessage
	for i, cmd := range txn.PendingReq {
		command := strings.ToLower(utils.B2S(cmd.Args[0]))
		txnHandler, ok := c.TxnHandle[command]
		if !ok {
			txn.Rollback()
			err := xerror.UnknownCommandError(command)
			WriteConnError(conn, EXEC_COMMAND, err)
			return err
		}
		resp := txnHandler.Func(txn, cmd.Args[1:])
		if txn.Err != nil {
			txn.Rollback()
			WriteConnError(conn, EXEC_COMMAND, txn.Err)
			return txn.Err
		}
		if command == PUBLISH_COMMAND {
			if pubindex == nil {
				pubindex = []int{i}
				pubMessage = []*PubSubMessage{resp.(*PubSubMessage)}
			} else {
				pubindex = append(pubindex, i)
				pubMessage = append(pubMessage, resp.(*PubSubMessage))
			}
		} else {
			ret[i] = resp
		}
	}

	if len(pubMessage) > 0 {
		err := c.psManager.WaitAlive()
		if err != nil {
			txn.Rollback()
			WriteConnError(conn, EXEC_COMMAND, err)
			return err
		}
	}

	// commit
	err := txn.Commit()
	if err != nil {
		if txn.Watch {
			conn.WriteNull()
			return err
		}

		txn.Rollback()
		WriteConnError(conn, EXEC_COMMAND, err)
		return err
	}

	if len(pubMessage) > 0 {
		counts := c.psManager.PublishMessages(pubMessage)
		for i, index := range pubindex {
			ret[index] = counts[i]
		}
	}

	// response
	txn.WriteAny(ret)
	return nil
}

func (c *Command) getTransaction(conn *redcon.Conn) (*store.Txn, bool) {
	connTxn := conn.Transaction()
	var txn *store.Txn
	if connTxn != nil {
		var ok bool
		txn, ok = connTxn.(*store.Txn)
		if ok {
			return txn, true
		}
	}
	return nil, false
}

func (c *Command) createTransaction(conn *redcon.Conn) *store.Txn {
	txn := c.client.NewTxn()
	cfg := c.GetConfig(conn.UserId)
	txn.Config = cfg
	txn.Conn = conn
	return txn
}

func (c *Command) getOrCreateTransaction(conn *redcon.Conn) (*store.Txn, bool) {
	txn, exist := c.getTransaction(conn)
	if exist {
		return txn, true
	}
	txn = c.createTransaction(conn)
	return txn, false
}

func (c *Command) multi(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("Multi", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		err := xerror.WrongArgsError(string(cmd.Args[0]))
		WriteConnError(conn, MULTI_COMMAND, err)
		return err
	}
	txn, alreadyExist := c.getOrCreateTransaction(conn)
	if txn.Multi {
		WriteConnError(conn, MULTI_COMMAND, xerror.ErrMultiNested)
		return xerror.ErrMultiNested
	}
	txn.PendingReq = make([]redcon.Command, 0)
	txn.Multi = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(OK)
	return nil
}

func (c *Command) watch(conn *redcon.Conn, cmd redcon.Command) error {
	utils.ZapLog.Debug("Watch", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) == 0 {
		err := xerror.WrongArgsError(string(cmd.Args[0]))
		WriteConnError(conn, WATCH_COMMAND, err)
		return err
	}

	txn, alreadyExist := c.getOrCreateTransaction(conn)
	if txn.Multi {
		WriteConnError(conn, WATCH_COMMAND, xerror.ErrWatchInsideMulti)
		return xerror.ErrWatchInsideMulti
	}

	if !txn.HasTransaction() {
		err := txn.Begin()
		if err != nil {
			WriteConnError(conn, WATCH_COMMAND, err)
			return err
		}
	}

	keys := make([][]byte, len(args))
	for i := range args {
		keys[i] = GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, args[i])
	}
	err := txn.LockKeys(keys)
	if err != nil {
		txn.Rollback()
		WriteConnError(conn, WATCH_COMMAND, err)
		return err
	}
	txn.Watch = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(OK)
	return nil
}

// UNWATCH
func (c *Command) UnWatchHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongArgs(UNWATCH_COMMAND)
	}
	txn.Watch = false
	return OK
}

// func WriteConnStringErr(conn *redcon.Conn, cmd, err string) {
// 	utils.ZapLog.Error("WriteConnError", zap.String("cmd", cmd), zap.String("error", err))
// 	metric.Metric.ErrorTotal.WithLabelValues(cmd).Inc()
// 	conn.WriteError(err)
// }

func WriteConnError(conn *redcon.Conn, cmd string, err error) {
	utils.ZapLog.Error("WriteConnError", zap.Error(err))
	metric.Metric.ErrorTotal.WithLabelValues(conn.UserName, strconv.Itoa(int(conn.DBId)), cmd).Inc()
	switch e := err.(type) {
	case *xerror.RedisError:
		conn.WriteError(e.StructError())
	default:
		conn.WriteError(err.Error())
	}
}
