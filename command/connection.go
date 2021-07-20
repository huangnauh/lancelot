package command

import (
	"bytes"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

// (connection) AUTH [username] password
func (c *Command) AuthHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(AUTH_COMMAND)
	} else if len(args) > 2 {
		return txn.SetError(xerror.ErrSyntax)
	}

	var username string
	var password []byte
	if len(args) == 1 {
		password = args[0]
	} else {
		username = utils.B2S(args[0])
		password = args[1]
	}

	if bytes.Equal(password, utils.S2B(c.cfg.Pass)) {
		if username == "" {
			username = c.cfg.Root
		} else if username != c.cfg.Root {
			return txn
		}
	}
	return nil
}
