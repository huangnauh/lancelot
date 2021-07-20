package command

import (
	"encoding/binary"
	"fmt"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/bitmap"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

const (
	AclHelpCommand = "ACL HELP"

	MAX_PASSWORDS = 10

	USER_FLAG_DISABLED    byte = 0
	USER_FLAG_ENABLED     byte = 1 << 0
	USER_FLAG_ALLKEYS     byte = 1 << 1
	USER_FLAG_ALLCHANNELS byte = 1 << 2
	USER_FLAG_ALLCOMMANDS byte = 1 << 3
	USER_FLAG_NOPASS      byte = 1 << 4
)

type User struct {
	Name      string
	Flag      byte
	Passwords map[string]bool
	Commands  *bitmap.Bitmap
}

func (c *Command) Resp(u *User) []interface{} {
	b := make([]interface{}, 10)

	b[0] = "flags"
	flags := make([]string, 0)
	if u.Flag&USER_FLAG_ENABLED != 0 {
		flags = append(flags, "on")
	} else {
		flags = append(flags, "off")
	}

	allKeys := false
	if u.Flag&USER_FLAG_ALLKEYS != 0 {
		allKeys = true
		flags = append(flags, "allkeys")
	}
	if u.Flag&USER_FLAG_ALLCHANNELS != 0 {
		flags = append(flags, "allchannels")
	}

	allCommands := false
	if u.Flag&USER_FLAG_ALLCOMMANDS != 0 {
		allCommands = true
		flags = append(flags, "allcommands")
	}
	if u.Flag&USER_FLAG_NOPASS != 0 {
		flags = append(flags, "nopass")
	}
	b[1] = flags

	b[2] = "passwords"
	passwords := make([]string, 0, len(u.Passwords))
	for password := range u.Passwords {
		passwords = append(passwords, password)
	}
	b[3] = passwords

	b[4] = "commands"
	var builder strings.Builder
	if allCommands {
		builder.WriteString("+@all")
		for name, handle := range c.TxnHandle {
			ok := u.Commands.IsSet(handle.ID)
			if !ok {
				builder.WriteString(" -")
				builder.WriteString(name)
			}
		}
	} else {
		builder.WriteString("-@all")
		for name, handle := range c.TxnHandle {
			ok := u.Commands.IsSet(handle.ID)
			if ok {
				builder.WriteString(" +")
				builder.WriteString(name)
			}
		}
	}
	b[5] = builder.String()

	b[6] = "keys"
	keys := make([]string, 0)
	if allKeys {
		keys = append(keys, "*")
	}
	b[7] = keys
	//TODO: ~<pattern> &<pattern>
	b[8] = "channels"
	b[9] = "*"
	return b
}

func UserEncode(u *User) []byte {
	k := make([]byte, 1+1+len(u.Passwords)*32+MAX_COMMANDS/8)
	k[0] = byte(u.Flag)
	k[1] = byte(len(u.Passwords))
	i := 0
	for password := range u.Passwords {
		utils.HexDecode(utils.S2B(password), k[2+i*32:])
		i++
	}
	for i, segment := range u.Commands.GetSegments() {
		binary.BigEndian.PutUint64(k[2+len(u.Passwords)*32+i*8:], segment)
	}
	return k
}

func UserDecode(b []byte, u *User) error {
	if len(b) < 2+MAX_COMMANDS/8 {
		return xerror.ErrValueTooShort
	}
	u.Flag = b[0]
	n := int(b[1])
	if len(b) < 2+MAX_COMMANDS/8+n*32 {
		return xerror.ErrValueTooShort
	}
	for i := 0; i < n; i++ {
		password := utils.B2S(utils.HexEncode(b[2+i*32 : 2+i*32+32]))
		u.Passwords[password] = true
	}
	segments := make([]uint64, MAX_COMMANDS/64)
	for i := 0; i < MAX_COMMANDS/64; i++ {
		segments[i] = binary.BigEndian.Uint64(b[2+n*32+i*8:])
	}
	u.Commands.SetSegments(segments)
	return nil
}

func (c *Command) AclHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(ACL_COMMAND)
	}

	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case SETUSER_COMMAND:
		return c.AclSetUser(txn, args[1:])
	case GETUSER_COMMAND:
		return c.AclGetUser(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, AclHelpCommand)
	}
}

// (server) ACL GETUSER username
func (c *Command) AclGetUser(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(GETUSER_COMMAND, AclHelpCommand)
	}
	key := GetKeyBytes(UserType, args[0])
	b, err := txn.Get(key)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	u := &User{}
	err = UserDecode(b, u)
	if err != nil {
		return txn.SetError(err)
	}
	return c.Resp(u)
}

// (server) ACL SETUSER username [rule [rule ...]]
func (c *Command) AclSetUser(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongSubArgs(SETUSER_COMMAND, AclHelpCommand)
	}

	u := &User{
		Name:      utils.B2S(args[0]),
		Passwords: make(map[string]bool),
		Commands:  bitmap.New(MAX_COMMANDS),
	}
	rules := args[1:]
	var err error
	for i := range rules {
		err = c.aclSetRule(txn, u, strings.ToLower(utils.B2S(rules[i])))
		if err != nil {
			return txn.SetError(err)
		}
	}
	key := GetKeyBytes(UserType, args[0])
	err = txn.Put(key, UserEncode(u))
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func getPassword(rule string) (string, error) {
	password := rule[1:]
	if len(password) != 64 {
		return "", xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
			rule, xerror.InvalidPassword)
	}
	_, err := utils.GetHexDecode(utils.S2B(password))
	if err != nil {
		return "", xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
			rule, xerror.InvalidPassword)
	}
	return password, nil
}

func (c *Command) aclSetRule(txn *store.Txn, u *User, rule string) error {
	switch rule {
	case "on":
		u.Flag |= USER_FLAG_ENABLED
	case "off":
		u.Flag &= ^USER_FLAG_ENABLED
	case "allkeys", "~*":
		u.Flag |= USER_FLAG_ALLKEYS
	case "resetkeys":
		u.Flag &= ^USER_FLAG_ALLKEYS
	case "allchannels", "&*":
		u.Flag |= USER_FLAG_ALLCHANNELS
	case "resetchannels":
		u.Flag &= ^USER_FLAG_ALLCHANNELS
	case "allcommands", "+@all":
		u.Flag |= USER_FLAG_ALLCOMMANDS
		u.Commands.SetFull()
	case "nocommands", "-@all":
		u.Flag &= ^USER_FLAG_ALLCOMMANDS
		u.Commands.SetEmpty()
	case "nopass":
		u.Flag |= USER_FLAG_NOPASS
	case "resetpass":
		u.Flag &= ^USER_FLAG_NOPASS
	default:
		if rule[0] == '>' {
			password := utils.Sha256Sum(utils.S2B(rule[1:]))
			if len(password) >= MAX_PASSWORDS {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
					rule, xerror.ErrTooManyPasswords)
			}
			u.Passwords[password] = true
		} else if rule[0] == '#' {
			password, err := getPassword(rule)
			if err != nil {
				return err
			}
			if len(password) >= MAX_PASSWORDS {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
					rule, xerror.ErrTooManyPasswords)
			}
			u.Passwords[password] = true
		} else if rule[0] == '<' {
			password := utils.Sha256Sum(utils.S2B(rule[1:]))
			ok := u.Passwords[password]
			if ok {
				delete(u.Passwords, password)
			} else {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
					rule, xerror.ErrNotExistPassword)
			}
		} else if rule[0] == '!' {
			password, err := getPassword(rule)
			if err != nil {
				return err
			}
			ok := u.Passwords[password]
			if ok {
				delete(u.Passwords, password)
			} else {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
					rule, xerror.ErrNotExistPassword)
			}
		} else if rule[0] == '+' {
			cmd := rule[1:]
			handle, ok := c.TxnHandle[cmd]
			if !ok {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND), rule, xerror.UnknownCommand)
			}
			u.Commands.Add(handle.ID)
		} else if rule[0] == '-' {
			cmd := rule[1:]
			handle, ok := c.TxnHandle[cmd]
			if !ok {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND), rule, xerror.UnknownCommand)
			}
			u.Commands.Remove(handle.ID)
		} else {
			// ~<pattern> &<pattern>
			return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND), rule, xerror.ErrNotSupport)
		}
	}
	return nil
}
