package xerror

import (
	"errors"
	"fmt"
)

var (
	ErrWatchInsideMulti = "ERROR WATCH inside MULTI is not allowed"
	ErrMultiNested      = "ERROR MULTI calls can not be nested"
	ErrEXECErr          = "ERROR EXEC without MULTI"
	ErrDISCARDErr       = "ERROR DISCARD without MULTI"
	ErrMultiErr         = "ERROR without MULTI"
	ErrTransactionErr   = "ERROR Transaction discarded because of previous errors."

	ErrNotInteger    = errors.New("ERROR value is not an integer or out of range")
	ErrInvalidExpire = errors.New("ERROR invalid expire time in set")
	ErrSyntax        = errors.New("ERROR syntax error")
	WrongTypeError   = errors.New("ERROR WRONGTYPE Operation against a key holding the wrong kind of value")

	MissingTxn        = errors.New("missing transcation")
	InvalidTxn        = errors.New("invalid transcation")
	WrongNumberOfArgs = errors.New("wrong number of arguments")

	ErrValueTooShort = errors.New("ERR value is too short")
	ErrNotTTL        = errors.New("ERR value is not ttl")
)

func WrongArgsString(command string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", command)
}

func WrongSubArgsString(command, help string) string {
	return fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try %s.",
		command, help)
}
