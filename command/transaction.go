package command

import (
	"fmt"
	"strings"
	"time"

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
	newTxn := c.client.NewTxn()
	newTxn.Conn = conn
	return newTxn, true
}

func (c *Command) SingleHandler(conn *redcon.Conn, txn *store.Txn, txnHandle TxnHandle, args [][]byte) error {
	err := txn.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	resp := txnHandle(txn, args)
	if txn.Err != nil {
		txn.WriteAny(resp)
		return nil
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	txn.WriteAny(resp)
	return nil
}

func (c *Command) TxnHandler(conn *redcon.Conn, comma string, cmd redcon.Command) {
	handler, ok := c.TxnHandle[comma]
	if !ok {
		txn, exist := c.getTransaction(conn)
		if exist && txn != nil && txn.Multi {
			txn.PendingErr = true
		}
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", comma))
		return
	}
	txnHandle := handler.Func
	args := cmd.Args[1:]
	txn, single := c.checkSingle(conn, comma == UNWATCH_COMMAND)
	utils.ZapLog.Debug("TxnHandler", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args), zap.Bool("single", single))
	if single {
		var err error
		for i := 0; i < 3; i++ {
			err = c.SingleHandler(conn, txn, txnHandle, args)
			if err == nil {
				return
			}
			time.Sleep(time.Millisecond * time.Duration(i+1))
		}
		writerConnError(conn, err)
		return
	}

	txn.PendingReq = append(txn.PendingReq, cmd)
	conn.WriteAny(Queued)

	// if txn.Exec {
	// 	if !txn.HasTransaction() {
	// 		txn.Err = xerror.InvalidTxn
	// 		writerConnError(conn, xerror.InvalidTxn)
	// 		return
	// 	}
	// 	resp := txnHandle(txn, args)
	// 	if txn.Err == nil {
	// 		txn.PendingResp = append(txn.PendingResp, resp)
	// 	}
	// } else {

	// }
}

func (c *Command) discard(conn *redcon.Conn, cmd redcon.Command) {
	utils.ZapLog.Debug("discard", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}
	txn, _ := c.getTransaction(conn)
	if txn == nil {
		conn.WriteError(xerror.ErrDISCARDErr)
		return
	}
	defer conn.SetTransaction(nil)
	if !txn.Multi {
		conn.WriteError(xerror.ErrDISCARDErr)
		return
	}
	txn.Rollback()
	conn.WriteAny(OK)
}

func (c *Command) exec(conn *redcon.Conn, cmd redcon.Command) {
	utils.ZapLog.Debug("Exec", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}

	defer conn.SetTransaction(nil)
	txn, exist := c.getTransaction(conn)
	if !exist || !txn.Multi {
		conn.WriteError(xerror.ErrEXECErr)
		return
	}
	if txn.PendingErr {
		conn.WriteError(xerror.ErrTransactionDiscarded)
		return
	}

	// not watch
	if !txn.HasTransaction() {
		err := txn.Begin()
		if err != nil {
			writerConnError(conn, err)
			return
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
			conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
			return
		}
		resp := txnHandler.Func(txn, cmd.Args[1:])
		if txn.Err != nil {
			txn.Rollback()
			writerConnError(conn, txn.Err)
			return
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
			writerConnError(conn, err)
			return
		}
	}

	// commit
	err := txn.Commit()
	if err != nil {
		if txn.Watch {
			conn.WriteNull()
			return
		}

		txn.Rollback()
		writerConnError(conn, err)
		return
	}

	if len(pubMessage) > 0 {
		counts := c.psManager.PublishMessages(pubMessage)
		for i, index := range pubindex {
			ret[index] = counts[i]
		}
	}

	// response
	txn.WriteAny(ret)
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

func (c *Command) multi(conn *redcon.Conn, cmd redcon.Command) {
	utils.ZapLog.Debug("Multi", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) != 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}
	txn, alreadyExist := c.getOrCreateTransaction(conn)
	if txn.Multi {
		conn.WriteError(xerror.ErrMultiNested)
		return
	}
	txn.PendingReq = make([]redcon.Command, 0)
	txn.Multi = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(OK)
}

func (c *Command) watch(conn *redcon.Conn, cmd redcon.Command) {
	utils.ZapLog.Debug("Watch", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	args := cmd.Args[1:]
	if len(args) == 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}

	txn, alreadyExist := c.getOrCreateTransaction(conn)
	if txn.Multi {
		conn.WriteError(xerror.ErrWatchInsideMulti)
		return
	}

	if !txn.HasTransaction() {
		err := txn.Begin()
		if err != nil {
			writerConnError(conn, err)
			return
		}
	}

	keys := make([][]byte, len(args))
	for i := range args {
		keys[i] = GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, args[i])
	}
	err := txn.LockKeys(keys)
	if err != nil {
		txn.Rollback()
		writerConnError(conn, err)
		return
	}
	txn.Watch = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(OK)
}

// UNWATCH
func (c *Command) UnWatchHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongArgs(UNWATCH_COMMAND)
	}
	txn.Watch = false
	return OK
}

func writerConnError(conn *redcon.Conn, err error) {
	conn.WriteError("Err " + err.Error())
}
