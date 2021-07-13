package store

import (
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func (txn *Txn) LazyWriteError(err error) RespFunc {
	txn.Err = err
	return func(txn *Txn) { txn.WriteError(err.Error()) }
}

func (txn *Txn) WriteWrongArgs(command string) {
	txn.WriteError(xerror.WrongArgsString(command))
}

func (txn *Txn) WriteWrongSubArgs(command, help string) {
	txn.WriteError(xerror.WrongSubArgsString(command, help))
}

func (txn *Txn) LazyWriteWrongArgs(command string) RespFunc {
	txn.Err = xerror.WrongNumberOfArgs
	return func(txn *Txn) { txn.WriteWrongArgs(command) }
}

func (txn *Txn) LazyWriteWrongSubArgs(command, help string) RespFunc {
	txn.Err = xerror.WrongNumberOfArgs
	return func(txn *Txn) { txn.WriteWrongSubArgs(command, help) }
}

func (txn *Txn) LazyWriteNull() RespFunc {
	return func(txn *Txn) {
		txn.WriteNull()
	}
}

func (txn *Txn) LazyWriteInt(num int) RespFunc {
	return func(txn *Txn) {
		txn.WriteInt(num)
	}
}

func (txn *Txn) LazyWriteInt64(num int64) RespFunc {
	return func(txn *Txn) {
		txn.WriteInt64(num)
	}
}

func (txn *Txn) LazyWriteBulk(value []byte) RespFunc {
	return func(txn *Txn) {
		txn.WriteBulk(value)
	}
}

func (txn *Txn) LazyWriteArrayBulk(value [][]byte) RespFunc {
	return func(txn *Txn) {
		txn.WriteArray(len(value))
		for i := range value {
			txn.LazyWriteBulk(value[i])
		}
	}
}

func (txn *Txn) LazyWriteString(value string) RespFunc {
	return func(txn *Txn) {
		txn.WriteString(value)
	}
}
