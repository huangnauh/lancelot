package server

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

func (s *Server) detach(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("detach: %v", cmd)
	detachedConn := conn.Detach()
	go func(c redcon.DetachedConn) {
		defer c.Close()

		c.WriteString(OK)
		c.Flush()
	}(detachedConn)
}

func (s *Server) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString(PONG)
}

func (s *Server) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString(OK)
	conn.Close()
}

func (s *Server) shutdown(conn redcon.Conn, cmd redcon.Command) {
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
	return newTxn, true
}

func (s *Server) Handler(conn redcon.Conn, cmd redcon.Command, txnHandle TxnHandle) {
	args := cmd.Args[1:]

	// txn, alreadyExist := s.getTransaction(conn)
	// single := !alreadyExist
	// if alreadyExist && !txn.Multi {
	// 	// after WATCH command but before MULTI command
	// 	newTxn = true
	// 	txn = s.client.NewTxn()
	// }

	txn, single := s.checkSingle(conn)
	logrus.Debugf("handler: %s, single: %t", cmd.Args, single)
	if single {
		err := txn.Begin()
		if err != nil {
			writerConnError(conn, err)
			return
		}
		resp := txnHandle(conn, txn, args)
		if txn.Err != nil {
			resp(conn)
			return
		}
		err = txn.Commit()
		if err != nil {
			writerConnError(conn, err)
			return
		}
		resp(conn)
		return
	}

	if txn.Exec {
		if !txn.HasTransaction() {
			txn.Err = invalidTxn
			writerConnError(conn, invalidTxn)
			return
		}
		resp := txnHandle(conn, txn, args)
		if txn.Err == nil {
			txn.PendingResp = append(txn.PendingResp, resp)
		}
	} else {
		txn.PendingReq = append(txn.PendingReq, cmd)
		conn.WriteString(Queued)
	}
}

func (s *Server) exec(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("exec: %v", cmd)
	args := cmd.Args[1:]
	if len(args) != 0 {
		writerConnWrongArgs(conn, string(cmd.Args[0]))
		return
	}

	txn, alreadyExist := s.getTransaction(conn)
	defer conn.SetTransaction(nil)

	if !txn.Multi || !alreadyExist {
		conn.WriteError(errEXECErr)
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
	conn.WriteArray(len(txn.PendingResp))
	for i := range txn.PendingResp {
		txn.PendingResp[i](conn)
	}
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
	return txn, false
}

func (s *Server) multi(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("multi: %v", cmd)
	args := cmd.Args[1:]
	if len(args) != 0 {
		writerConnWrongArgs(conn, string(cmd.Args[0]))
		return
	}
	txn, alreadyExist := s.getTransaction(conn)
	if txn.Multi {
		conn.WriteError(errMultiNested)
		return
	}
	txn.PendingReq = make([]redcon.Command, 0)
	txn.Multi = true
	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteString(OK)
}

func (s *Server) watch(conn redcon.Conn, cmd redcon.Command) {
	logrus.Debugf("watch: %v", cmd)
	args := cmd.Args[1:]
	if len(args) == 0 {
		writerConnWrongArgs(conn, string(cmd.Args[0]))
		return
	}

	txn, alreadyExist := s.getTransaction(conn)
	if txn.Multi {
		conn.WriteError(errWatchInsideMulti)
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
		writerConnError(conn, err)
		return
	}

	if !alreadyExist {
		conn.SetTransaction(txn)
	}
	conn.WriteString(OK)
}

func writerConnError(conn redcon.Conn, err error) {
	conn.WriteError("Err " + err.Error())
}

func writerConnWrongArgs(conn redcon.Conn, command string) {
	conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
}
