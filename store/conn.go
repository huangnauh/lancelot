package store

import (
	"errors"
	"fmt"
)

var (
	WrongNumberOfArgs = errors.New("wrong number of arguments")
)

func (txn *Txn) LazyWriteError(err error) RespFunc {
	txn.Err = err
	return func(txn *Txn) { txn.WriteError(err.Error()) }
}

func (txn *Txn) WriteWrongArgs(command string) {
	txn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
}

func (txn *Txn) WriteWrongSubArgs(command, help string) {
	txn.WriteError(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try %s.",
		command, help))
}

func (txn *Txn) LazyWriteWrongArgs(command string) RespFunc {
	txn.Err = WrongNumberOfArgs
	return func(txn *Txn) { txn.WriteWrongArgs(command) }
}

func (txn *Txn) LazyWriteWrongSubArgs(command, help string) RespFunc {
	txn.Err = WrongNumberOfArgs
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
