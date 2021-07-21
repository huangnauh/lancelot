package command

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/bitmap"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

const (
	AclHelpCommand = "ACL HELP"

	USER_FLAG_ENABLED     byte = 1 << 0
	USER_FLAG_ALLKEYS     byte = 1 << 1
	USER_FLAG_ALLCHANNELS byte = 1 << 2
	USER_FLAG_ALLCOMMANDS byte = 1 << 3
	USER_FLAG_NOPASS      byte = 1 << 4

	UserFlagRoot byte = USER_FLAG_ENABLED | USER_FLAG_ALLKEYS | USER_FLAG_ALLCHANNELS | USER_FLAG_ALLCOMMANDS
)

type User struct {
	Name      string
	Flag      byte
	Passwords map[string]bool
	Commands  *bitmap.Bitmap
}

type ByName []*User

func (a ByName) Len() int {
	return len(a)
}
func (a ByName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByName) Less(i, j int) bool {
	return a[j].Name < a[i].Name
}

func (c *Command) RootUser() *User {
	root := &User{
		Name: c.cfg.Auth.Root,
		Flag: UserFlagRoot,
		Passwords: map[string]bool{
			utils.Sha256Sum(utils.S2B(c.cfg.Auth.Pass)): true,
		},
		Commands: bitmap.New(MAX_COMMANDS),
	}
	root.Commands.SetFull()
	return root
}

func (c *Command) ListResp(u *User) string {
	var builder strings.Builder
	builder.WriteString("user ")
	builder.WriteString(u.Name)
	if u.Flag&USER_FLAG_ENABLED != 0 {
		builder.WriteString(" on")
	} else {
		builder.WriteString(" off")
	}

	if u.Flag&USER_FLAG_NOPASS != 0 {
		builder.WriteString(" nopass")
	} else {
		for password := range u.Passwords {
			builder.WriteString(" #")
			builder.WriteString(password)
		}
	}

	if u.Flag&USER_FLAG_ALLKEYS != 0 {
		builder.WriteString(" ~*")
	}

	if u.Flag&USER_FLAG_ALLCHANNELS != 0 {
		builder.WriteString(" &*")
	}

	if u.Flag&USER_FLAG_ALLCOMMANDS != 0 {
		builder.WriteString(" +@all")
		for name, handle := range c.TxnHandle {
			ok := u.Commands.IsSet(handle.ID)
			if !ok {
				builder.WriteString(" -")
				builder.WriteString(name)
			}
		}
	} else {
		builder.WriteString(" -@all")
		for name, handle := range c.TxnHandle {
			ok := u.Commands.IsSet(handle.ID)
			if ok {
				builder.WriteString(" +")
				builder.WriteString(name)
			}
		}
	}
	return builder.String()
}

func (c *Command) GetResp(u *User) []interface{} {
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
		_ = utils.HexDecode(utils.S2B(password), k[2+i*32:])
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

	u.Passwords = make(map[string]bool, n)
	for i := 0; i < n; i++ {
		password := utils.B2S(utils.HexEncode(b[2+i*32 : 2+i*32+32]))
		u.Passwords[password] = true
	}
	segments := make([]uint64, MAX_COMMANDS/64)
	for i := 0; i < MAX_COMMANDS/64; i++ {
		segments[i] = binary.BigEndian.Uint64(b[2+n*32+i*8:])
	}
	u.Commands = new(bitmap.Bitmap)
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
	case LIST_COMMAND:
		return c.AclList(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, AclHelpCommand)
	}
}

func (c *Command) ListUsers() (map[string]*User, error) {
	prefix := GetKeyPrefix(UserType)

	users := make(map[string]*User)
	callback := func(key, value []byte) bool {
		username := string(key[len(prefix):])
		u := &User{Name: username}
		if err := UserDecode(value, u); err != nil {
			return true
		}
		users[username] = u
		return true
	}
	err := c.client.List(prefix, utils.PrefixNext(prefix), c.cfg.Auth.MaxUsers, callback)
	return users, err
}

func (c *Command) GetUser(txn *store.Txn, username string) (*User, error) {
	key := GetKeyBytes(UserType, utils.S2B(username))
	b, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	u := &User{Name: username}
	err = UserDecode(b, u)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// (server) ACL LIST
func (c *Command) AclList(txn *store.Txn, args [][]byte) interface{} {
	users := c.GetLocalUsers()
	sort.Sort(ByName(users))
	b := make([]string, len(users))
	i := 0
	for _, user := range users {
		b[i] = c.ListResp(user)
		i++
	}
	return b
}

// (server) ACL GETUSER username
func (c *Command) AclGetUser(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(GETUSER_COMMAND, AclHelpCommand)
	}

	u, ok := c.GetLocalUser(utils.B2S(args[0]))
	if !ok {
		return nil
	}
	return c.GetResp(u)
}

// (server) ACL SETUSER username [rule [rule ...]]
func (c *Command) AclSetUser(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongSubArgs(SETUSER_COMMAND, AclHelpCommand)
	}

	username := utils.B2S(args[0])
	u, err := c.GetUser(txn, username)
	if err == store.KeyNotFound {
		u = &User{
			Name:      username,
			Flag:      USER_FLAG_ALLCHANNELS,
			Passwords: make(map[string]bool),
			Commands:  bitmap.New(MAX_COMMANDS),
		}
	} else if err != nil {
		return txn.SetError(err)
	}
	c.SetLocalUser(u)

	rules := args[1:]
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
			if len(password) >= c.cfg.Auth.MaxPasswordsPerUser {
				return xerror.WrongModifier(fmt.Sprintf("%s %s", ACL_COMMAND, SETUSER_COMMAND),
					rule, xerror.ErrTooManyPasswords)
			}
			u.Passwords[password] = true
		} else if rule[0] == '#' {
			password, err := getPassword(rule)
			if err != nil {
				return err
			}
			if len(password) >= c.cfg.Auth.MaxPasswordsPerUser {
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
