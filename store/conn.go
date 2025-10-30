package store

import (
	"github.com/huangnauh/lancelot/xerror"
)

func (txn *Txn) SetError(err error) error {
	txn.Err = err
	return err
}

func (txn *Txn) SetWrongArgs(command string) error {
	err := xerror.WrongArgsError(command)
	return txn.SetError(err)
}

func (txn *Txn) SetWrongSubArgs(command, help string) error {
	err := xerror.WrongSubArgsError(command, help)
	return txn.SetError(err)
}
