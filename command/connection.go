package command

import (
	"fmt"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
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
					txn.UserName = user.Name
					return OK
				}
			}
		}
	}
	return txn.SetError(xerror.WRONGPASS)
}

// (connection) SELECT index
func (c *Command) SelectHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(SELECT_COMMAND)
	}

	index, err := utils.GetNonnegativeInt64(args[0])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}

	txn.DBId = uint8(index)
	return OK
}

// CLIENT
func (c *Command) ClientHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(CLIENT_COMMAND)
	}
	str := strings.ToLower(utils.B2S(args[0]))
	switch str {
	case ID_COMMAND:
		return c.ClientIdHandle(txn, args[1:])
	case SETNAME_COMMAND:
		return c.ClientSetNameHandle(txn, args[1:])
	case GETNAME_COMMAND:
		return c.ClientGetNameHandle(txn, args[1:])
	case INFO_COMMAND:
		return c.ClientInfoHandle(txn, args[1:])
	case KILL_COMMAND:
		return c.ClientKillHandle(txn, args[1:])
	case LIST_COMMAND:
		return c.ClientListHandle(txn, args[1:])
	default:
		return txn.SetError(xerror.ErrSyntax)
	}
}

// CLIENT ID
func (c *Command) ClientIdHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, ID_COMMAND)
	}
	return redcon.SimpleInt(txn.ID)
}

const (
	ClientInfoFormat = "id=%d addr=%s laddr=%s:%d fd=%d " +
		"name=%s age=%d idle=%d " +
		"flags=%s db=%d sub=%d psub=%d multi=%d qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=rq cmd=client user=%s redir=-1\n"
)

// CLIENT INFO
func (c *Command) ClientInfoHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, INFO_COMMAND)
	}

	flags := "N"
	multi := -1
	if txn.Multi {
		flags = "x"
		multi = len(txn.PendingReq)
	}

	sub, psub := c.psManager.Channels()
	return fmt.Sprintf(ClientInfoFormat, txn.ID, txn.RemoteAddr(), c.cfg.Host, c.cfg.RedisPort, 0,
		txn.Name, time.Since(txn.CreatedAt)/time.Second, time.Since(txn.UpdatedAt)/time.Second,
		flags, txn.DBId, sub, psub, multi, txn.UserName)
}

// CLIENT SETNAME connection-name
func (c *Command) ClientSetNameHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, SETNAME_COMMAND)
	}
	txn.Name = utils.B2S(args[0])
	return OK
}

// CLIENT GETNAME
func (c *Command) ClientGetNameHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, GETNAME_COMMAND)
	}
	return txn.Name
}

// CLIENT LIST [TYPE normal|master|replica|pubsub] [ID client-id [client-id ...]]
func (c *Command) ClientListHandle(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

// CLIENT KILL [ip:port] [ID client-id] [TYPE normal|master|slave|pubsub] [USER username] [ADDR ip:port] [LADDR ip:port] [SKIPME yes/no]
func (c *Command) ClientKillHandle(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

// ECHO message
func (c *Command) EchoHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(ECHO_COMMAND)
	}
	return args[0]
}

// PING [message]
func (c *Command) PingHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongArgs(PING_COMMAND)
	}
	if len(args) == 0 {
		return PONG
	}
	return args[0]
}
