package server

import (
	"github.com/tidwall/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

func Get(conn redcon.Conn, txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 1 {
		txn.Err = wrongNumberOfArgs
		return func(conn redcon.Conn) {
			writerConnWrongArgs(conn, GET_COMMAND)
		}
	}
	key := args[0]
	value, err := txn.Get(key)
	if err == store.KeyNotFound {
		return func(conn redcon.Conn) {
			conn.WriteNull()
		}
	} else if err != nil {
		txn.Err = err
		return func(conn redcon.Conn) {
			writerConnError(conn, err)
		}
	} else {
		return func(conn redcon.Conn) {
			conn.WriteString(string(value))
		}
	}
}

func Set(conn redcon.Conn, txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 2 {
		txn.Err = wrongNumberOfArgs
		return func(conn redcon.Conn) {
			writerConnWrongArgs(conn, SET_COMMAND)
		}
	}
	key := args[0]
	value := args[1]
	err := txn.Put(key, value)
	if err != nil {
		txn.Err = err
		return func(conn redcon.Conn) {
			writerConnError(conn, err)
		}
	} else {
		return func(conn redcon.Conn) {
			conn.WriteString(OK)
		}
	}
}
