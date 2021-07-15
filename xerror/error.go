package xerror

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrWatchInsideMulti = "ERROR WATCH inside MULTI is not allowed"
	ErrMultiNested      = "ERROR MULTI calls can not be nested"
	ErrEXECErr          = "ERROR EXEC without MULTI"
	ErrDISCARDErr       = "ERROR DISCARD without MULTI"
	ErrMultiErr         = "ERROR without MULTI"
	ErrTransactionErr   = "ERROR Transaction discarded because of previous errors."

	ErrNotInteger          = errors.New("ERROR value is not an integer or out of range")
	ErrNumberGreater       = errors.New("ERROR Number of keys can't be greater than number of args")
	ErrNumberNegative      = errors.New("ERROR Number of keys can't be negative")
	ErrInvalidExpire       = errors.New("ERROR invalid expire time in set")
	ErrLuaInvalidType      = errors.New("ERROR lua invalid type")
	ErrSyntax              = errors.New("ERROR syntax error")
	WrongTypeError         = errors.New("ERROR WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoMatchScript       = errors.New("ERROR No matching script. Please use EVAL.")
	ErrReadOnlyScript      = errors.New("ERROR Write commands are not allowed from read-only scripts.")
	UnsupportCmdFromScript = errors.New("ERROR Unsupported commands from scripts.")

	MissingTxn        = errors.New("missing transcation")
	InvalidTxn        = errors.New("invalid transcation")
	WrongNumberOfArgs = errors.New("wrong number of arguments")

	ErrValueTooShort = errors.New("ERR value is too short")
	ErrNotTTL        = errors.New("ERR value is not ttl")

	ErrNoLuasAvailable = errors.New("ERR no lua available")
)

func WrongArgsString(command string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", command)
}

func WrongArgsError(command string) error {
	return errors.New(WrongArgsString(command))
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
