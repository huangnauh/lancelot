package xerror

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrWatchInsideMulti    = "WATCH inside MULTI is not allowed"
	ErrMultiNested         = "MULTI calls can not be nested"
	ErrEXECErr             = "EXEC without MULTI"
	ErrDISCARDErr          = "DISCARD without MULTI"
	ErrMultiErr            = "without MULTI"
	ErrTransactionErr      = "Transaction discarded because of previous errors."
	ErrNotInteger          = errors.New("value is not an integer or out of range")
	ErrNotFloat            = errors.New("value is not a valid float")
	ErrOffset              = errors.New("offset is out of range")
	ErrNumberGreater       = errors.New("Number of keys can't be greater than number of args")
	ErrNumberNegative      = errors.New("Number of keys can't be negative")
	ErrLuaInvalidType      = errors.New("lua invalid type")
	ErrSyntax              = errors.New("syntax error")
	ErrNotExpire           = errors.New("not expire")
	WrongTypeError         = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoMatchScript       = errors.New("No matching script. Please use EVAL.")
	ErrReadOnlyScript      = errors.New("Write commands are not allowed from read-only scripts.")
	UnsupportCmdFromScript = errors.New("Unsupported commands from scripts.")
	UnsupportFlushOption   = errors.New("SCRIPT FLUSH only support SYNC|ASYNC option.")
	WRONGPASS              = errors.New("WRONGPASS invalid username-password pair or user is disabled.")
	ErrAuthentication      = errors.New("Authentication required.")

	MissingTxn        = errors.New("missing transcation")
	InvalidTxn        = errors.New("invalid transcation")
	WrongNumberOfArgs = errors.New("wrong number of arguments")

	ErrValueTooShort       = errors.New("ERR value is too short")
	ErrNotTTL              = errors.New("ERR value is not ttl")
	ErrTTLInvalidValueType = errors.New("ERR ttl invalid value type")

	ErrNoLuasAvailable   = errors.New("ERR no lua available")
	ErrNotSupport        = errors.New("not support")
	InvalidPassword      = errors.New("The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters")
	ErrNotExistPassword  = errors.New("The password you are trying to remove from the user does not exist")
	ErrTooManyPasswords  = errors.New("Too many passwords")
	UnknownCommandInACL  = errors.New("Unknown command or category name in ACL")
	ErrCheckFailed       = errors.New("check failed")
	InvalidJsonError     = errors.New("invalid json")
	InvalidJsonPathError = errors.New("invalid json path")
	InvalidCursor        = errors.New("invalid cursor")
	// KeyTimeOut           = errors.New("key time out")

	InvalidOffset = errors.New("invalid offset")
	ErrClosed     = errors.New("closed")
)

func WrongArgsString(command string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", command)
}

func NotExistKey(key string) string {
	return fmt.Sprintf("key '%s' does not exist in path", key)
}

func UnknownCommand(command string) string {
	return fmt.Sprintf("unknown command `%s`, with args beginning with:", command)

}

func InvalidExpire(command string) string {
	return fmt.Sprintf("invalid expire time in %s", command)
}

func InvalidCommand(command string) string {
	return fmt.Sprintf("ERR Can't execute '%s': "+
		"only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are "+
		"allowed in this context", command)
}

func WrongModifier(command, modifier string, err error) error {
	return fmt.Errorf("Error in %s modifier '%s': %s", command, modifier, err.Error())
}

func NotExistKeyError(key string) error {
	return errors.New(NotExistKey(key))
}

func WrongArgsError(command string) error {
	return errors.New(WrongArgsString(command))
}

func UnknownCommandError(command string) error {
	return errors.New(UnknownCommand(command))
}

func InvalidExpireError(command string) error {
	return errors.New(InvalidExpire(command))
}

func InvalidCommandError(command string) error {
	return errors.New(InvalidCommand(command))
}

func WrongSubArgsString(command, help string) string {
	return fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try %s.",
		command, help)
}

func WrongSubArgsError(command, help string) error {
	return errors.New(WrongSubArgsString(command, help))
}

func MakeSafeErr(err error) error {
	return errors.New(strings.Replace(err.Error(), "\n", `\n`, -1))
}
