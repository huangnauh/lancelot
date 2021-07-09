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

func (txn *Txn) LazyWriteWrongArgs(command string) RespFunc {
	txn.Err = WrongNumberOfArgs
	return func(txn *Txn) { txn.WriteWrongArgs(command) }
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

func (txn *Txn) LazyWriteString(value string) RespFunc {
	return func(txn *Txn) {
		txn.WriteString(value)
	}
}
