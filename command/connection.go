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
		if user.Flag&USER_FLAG_ENABLED == 0 {
			continue
		}
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
	if len(args) < 1 {
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
	case UNBLOCK_COMMAND:
		return c.ClientUnblockHandle(txn, args[1:])
	default:
		return txn.SetError(xerror.ErrSyntax)
	}
}

// CLIENT ID
func (c *Command) ClientIdHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, ID_COMMAND)
	}
	return redcon.SimpleInt(txn.ID)
}

const (
	ClientInfoFormat = "id=%d addr=%s laddr=%s fd=%d " +
		"name=%s age=%d idle=%d " +
		"flags=%s db=%d sub=%d psub=%d multi=%d qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=rq cmd=%s user=%s redir=-1\n"
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
	return fmt.Sprintf(ClientInfoFormat, txn.ID, txn.RemoteAddr(), c.red.Addr(), 0,
		txn.Name, time.Since(txn.CreatedAt)/time.Second, time.Since(txn.UpdatedAt)/time.Second,
		flags, txn.DBId, sub, psub, multi, txn.LastCmd, txn.UserName)
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
	conns := c.red.Conns()
	var b strings.Builder
	now := time.Now()
	for _, conn := range conns {
		if txn.UserId != c.Root.ID && conn.UserId != txn.UserId {
			continue
		}
		flags := "N"
		if conn.Blocked {
			flags = "b"
		}
		b.WriteString(fmt.Sprintf(ClientInfoFormat, conn.ID, conn.RemoteAddr(), c.red.Addr(), 0,
			conn.Name, now.Sub(conn.CreatedAt)/time.Second, now.Sub(conn.UpdatedAt)/time.Second,
			flags, conn.DBId, 0, 0, -1, conn.LastCmd, conn.UserName))
	}
	return b.String()
}

func (c *Command) ClientUnblockHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongSubArgs(CLIENT_COMMAND, UNBLOCK_COMMAND)
	}
	id, err := utils.GetNonnegativeInt64(args[0])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	conn, _ := c.red.GetConnByID(uint64(id))
	if conn == nil {
		return redcon.SimpleInt(0)
	}
	if txn.UserId != c.Root.ID && txn.UserId != conn.UserId {
		return txn.SetError(xerror.WrongPermissionError(CLIENT_COMMAND))
	}
	if !conn.Blocked {
		return redcon.SimpleInt(0)
	}
	trigger := redcon.TimeoutTrigger
	if len(args) > 1 {
		tr := strings.ToLower(utils.B2S(args[1]))
		if tr == "error" {
			trigger = redcon.ErrorTrigger
		}
	}
	select {
	case conn.Trigger <- trigger:
		return redcon.SimpleInt(1)
	default:
		return redcon.SimpleInt(0)
	}
}

// CLIENT KILL [ip:port] [ID client-id] [TYPE normal|master|slave|pubsub] [USER username] [ADDR ip:port] [LADDR ip:port] [SKIPME yes/no]
func (c *Command) ClientKillHandle(txn *store.Txn, args [][]byte) interface{} {
	var uid uint64
	var username string
	skipme := true
	for i := 0; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case "id":
			if len(args) <= i+1 {
				return txn.SetError(xerror.ErrSyntax)
			}
			id, err := utils.GetNonnegativeInt64(args[i+1])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			uid = uint64(id)
			i++
		case "user":
			if len(args) <= i+1 {
				return txn.SetError(xerror.ErrSyntax)
			}
			username = utils.B2S(args[i+1])
			i++
		case "skipme":
			if len(args) <= i+1 {
				return txn.SetError(xerror.ErrSyntax)
			}
			s := strings.ToLower(utils.B2S(args[i+1]))
			if s == "yes" {
				skipme = true
			} else if s == "no" {
				skipme = false
			} else {
				return txn.SetError(xerror.ErrSyntax)
			}
			i++
		}
	}

	if uid != 0 {
		if skipme && txn.ID == uid {
			return redcon.SimpleInt(0)
		}
		conn, _ := c.red.GetConnByID(uid)
		if conn == nil {
			return redcon.SimpleInt(0)
		}
		if txn.UserId != c.Root.ID && txn.UserId != conn.UserId {
			return txn.SetError(xerror.WrongPermissionError(CLIENT_COMMAND))
		}
		conn.Closed()
		return redcon.SimpleInt(1)
	}

	count := 0
	conns := c.red.Conns()
	for _, conn := range conns {
		if skipme && conn.ID == txn.ID {
			continue
		}

		if txn.UserId != c.Root.ID && txn.UserId != conn.UserId {
			return txn.SetError(xerror.WrongPermissionError(CLIENT_COMMAND))
		}
		if conn.Name == username {
			conn.Close()
			count++
		}
	}
	return redcon.SimpleInt(count)
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
	num := c.luapool.GetWorkingScript()
	if num >= c.cfg.Lua.MaxPoolSize {
		return BUSYPONG
	}

	if len(args) == 0 {
		return PONG
	}
	return args[0]
}
