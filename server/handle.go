package server

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func (s *Server) detach(conn store.Txn, cmd redcon.Command) {
	logrus.Debugf("detach: %v", cmd)
	detachedConn := conn.Detach()
	go func(c redcon.DetachedConn) {
		defer c.Close()

		c.WriteAny(command.OK)
		c.Flush()
	}(detachedConn)
}

func (s *Server) ping(conn store.Txn, cmd redcon.Command) {
	conn.WriteAny(command.PONG)
}

func (s *Server) quit(conn store.Txn, cmd redcon.Command) {
	conn.WriteAny(command.OK)
	conn.Close()
}

func (s *Server) shutdown(conn store.Txn, cmd redcon.Command) {
	conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.Shutdown(ctx)
}

func (s *Server) checkSingle(conn redcon.Conn) (*store.Txn, bool) {
	connTxn := conn.Transaction()
	var txn *store.Txn
	if connTxn != nil {
		var ok bool
		txn, ok = connTxn.(*store.Txn)
		if ok && txn.Multi {
			return txn, false
		}
		// after WATCH command but before MULTI command
	}
	newTxn := s.client.NewTxn()
	newTxn.Conn = conn
	return newTxn, true
}

func (s *Server) Handler(conn redcon.Conn, cmd redcon.Command, txnHandle command.TxnHandle) {
	args := cmd.Args[1:]
	txn, single := s.checkSingle(conn)
	logrus.Debugf("handler: %s, single: %t", cmd.Args, single)
	if single {
		err := txn.Begin()
		if err != nil {
			writerConnError(conn, err)
			return
		}
		resp := txnHandle(txn, args)
		if txn.Err != nil {
			txn.Rollback()
			txn.WriteAny(resp)
			return
		}
		err = txn.Commit()
		if err != nil {
			writerConnError(conn, err)
			return
		}
		txn.WriteAny(resp)
		return
	}

	if txn.Exec {
		if !txn.HasTransaction() {
			txn.Err = xerror.InvalidTxn
			writerConnError(conn, xerror.InvalidTxn)
			return
		}
		resp := txnHandle(txn, args)
		if txn.Err == nil {
			txn.PendingResp = append(txn.PendingResp, resp)
		}
	} else {
		txn.PendingReq = append(txn.PendingReq, cmd)
		conn.WriteAny(command.Queued)
	}
}

func (s *Server) exec(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("exec: %v", cmd)
	args := cmd.Args[1:]
	if len(args) != 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}

	txn, alreadyExist := s.getTransaction(conn)
	defer conn.SetTransaction(nil)

	if !txn.Multi || !alreadyExist {
		conn.WriteError(xerror.ErrEXECErr)
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
	for _, cmd := range txn.PendingReq {
		s.ServeRESP(conn, cmd)
		if txn.Err != nil {
			txn.Rollback()
			writerConnError(conn, txn.Err)
			return
		}
	}

	// commit
	err := txn.Commit()
	if err != nil {
		writerConnError(conn, err)
		return
	}

	// response
	txn.WriteAny(txn.PendingResp)
	// txn.WriteArray(len(txn.PendingResp))
	// for i := range txn.PendingResp {
	// 	txn.WriteAny(txn.PendingResp[i])
	// }
}

func (s *Server) getTransaction(conn redcon.Conn) (*store.Txn, bool) {
	connTxn := conn.Transaction()
	var txn *store.Txn
	if connTxn != nil {
		var ok bool
		txn, ok = connTxn.(*store.Txn)
		if ok {
			return txn, true
		}
	}
	txn = s.client.NewTxn()
	txn.Conn = conn
	return txn, false
}

func (s *Server) multi(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("multi: %v", cmd)
	args := cmd.Args[1:]
	if len(args) != 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}
	txn, alreadyExist := s.getTransaction(conn)
	if txn.Multi {
		conn.WriteError(xerror.ErrMultiNested)
		return
	}
	txn.PendingReq = make([]redcon.Command, 0)
	txn.Multi = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(command.OK)
}

func (s *Server) watch(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("watch: %v", cmd)
	args := cmd.Args[1:]
	if len(args) == 0 {
		conn.WriteError(xerror.WrongArgsString(string(cmd.Args[0])))
		return
	}

	txn, alreadyExist := s.getTransaction(conn)
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
		keys[i] = []byte(args[i])
	}
	err := txn.LockKeys(keys)
	if err != nil {
		txn.Rollback()
		writerConnError(conn, err)
		return
	}

	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteAny(command.OK)
}

func writerConnError(conn redcon.Conn, err error) {
	conn.WriteError("Err " + err.Error())
}
