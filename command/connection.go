package command

import (
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

	pass := utils.Sha256Sum(password)

	users := c.GetLocalUsers()
	for _, user := range users {
		if username == "" || user.Name == username {
			for p := range user.Passwords {
				if p == pass {
					txn.Auth = true
					txn.UserId = user.ID
					return OK
				}
			}
		}
	}
	return txn.SetError(xerror.WRONGPASS)
}
