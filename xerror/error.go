package xerror

import (
	"errors"
	"fmt"
	"strings"
)

type Error interface {
	Error() string
	SetError(string)
	StructError() string
}

type RedisError struct {
	Prefix string
	Name   string
}

func (e *RedisError) Error() string {
	return e.Name
}

func (e *RedisError) StructError() string {
	return fmt.Sprintf("%s %s", e.Prefix, e.Name)
}

func (e *RedisError) SetError(name string) {
	e.Name = name
}

func ScriptNew(text string) *RedisError {
	return &RedisError{Prefix: "NOSCRIPT", Name: text}
}

func RedisNew(text string) *RedisError {
	return &RedisError{Prefix: "ERR", Name: text}
}

func WrongTypeNew(text string) *RedisError {
	return &RedisError{Prefix: "WRONGTYPE", Name: text}
}

func BusyNew(text string) *RedisError {
	return &RedisError{Prefix: "BUSY", Name: text}
}

func ExecAbortNew(text string) *RedisError {
	return &RedisError{Prefix: "EXECABORT", Name: text}
}

var (
	ErrWatchInsideMulti     = RedisNew("WATCH inside MULTI is not allowed")
	ErrMultiNested          = RedisNew("MULTI calls can not be nested")
	ErrEXECErr              = RedisNew("EXEC without MULTI")
	ErrDISCARDErr           = RedisNew("DISCARD without MULTI")
	ErrMultiErr             = RedisNew("without MULTI")
	ErrTransactionDiscarded = ExecAbortNew("Transaction discarded because of previous errors.")

	ErrNotInteger          = RedisNew("value is not an integer or out of range")
	ErrGTLTCompat          = RedisNew("GT and LT options at the same time are not compatible")
	ErrGTLTNXCompat        = RedisNew("NX and XX, GT or LT options at the same time are not compatible")
	ErrInvalidFloat        = RedisNew("value is not a valid float")
	ErrNotPositiveInteger  = RedisNew("value is out of range, must be positive")
	ErrRankZero            = RedisNew("RANK can't be zero: use 1 to start from the first match, 2 from the second, ...")
	ErrBitFieldType        = RedisNew("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
	ErrNotFloat            = RedisNew("value is not a valid float")
	ErrOffset              = RedisNew("offset is out of range")
	ErrFloatInfinity       = RedisNew("increment would produce NaN or Infinity")
	ErrNumberGreater       = RedisNew("Number of keys can't be greater than number of args")
	ErrNumberNegative      = RedisNew("Number of keys can't be negative")
	ErrCountNegative       = RedisNew("COUNT can't be negative")
	ErrTimeoutNegative     = RedisNew("timeout is negative")
	ErrLuaInvalidType      = RedisNew("lua invalid type")
	ErrSyntax              = RedisNew("syntax error")
	ErrMinMaxString        = RedisNew("min or max not valid string range item")
	ErrNotExpire           = RedisNew("not expire")
	WrongTypeErr           = WrongTypeNew("Operation against a key holding the wrong kind of value")
	ErrNoMatchScript       = ScriptNew("No matching script. Please use EVAL.")
	ErrReadOnlyScript      = RedisNew("Write commands are not allowed from read-only scripts.")
	CallNeedArgsScript     = RedisNew("Please specify at least one argument for redis.call()")
	UnsupportCmdFromScript = RedisNew("This Redis command is not allowed from scripts")
	UnknownCmdFromScript   = RedisNew("Unknown Redis command called from Lua script")
	UnsupportFlushOption   = RedisNew("SCRIPT FLUSH only support SYNC|ASYNC option.")
	WRONGPASS              = RedisNew("WRONGPASS invalid username-password pair or user is disabled.")
	ErrAuthentication      = RedisNew("Authentication required.")
	ErrExceedMaxSize       = RedisNew("exceeds maximum allowed size")
	ErrBusyScript          = BusyNew("Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.")
	ErrScriptKilled        = RedisNew("Script killed by user with SCRIPT KILL...")
	ErrScriptTimeout       = RedisNew("Script timeout")
	ErrResultNan           = RedisNew("resulting score is not a number (NaN)")
	ErrStoreOption         = RedisNew("STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options")

	MissingTxn        = RedisNew("missing transcation")
	InvalidTxn        = RedisNew("invalid transcation")
	WrongNumberOfArgs = RedisNew("wrong number of arguments")

	ErrValueTooShort       = RedisNew("value is too short")
	ErrKeyTooLong          = RedisNew("key is too long")
	ErrValueTooLong        = RedisNew("value is too long")
	ErrNotTTL              = RedisNew("value is not ttl")
	ErrTTLInvalidValueType = RedisNew("ttl invalid value type")

	ErrNoLuasAvailable         = RedisNew("no lua available")
	ErrNotSupport              = RedisNew("not support")
	InvalidPassword            = RedisNew("The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters")
	ErrNotExistPassword        = RedisNew("The password you are trying to remove from the user does not exist")
	ErrTooManyPasswords        = RedisNew("Too many passwords")
	ErrInvalidPassword         = RedisNew("invalid password")
	ErrSubscribeMessageTooSlow = RedisNew("subscribe message too slow")
	UnknownCommandInACL        = RedisNew("Unknown command or category name in ACL")
	ErrCheckFailed             = RedisNew("check failed")
	InvalidJsonError           = RedisNew("invalid json")
	InvalidJsonPathError       = RedisNew("invalid json path")
	InvalidCursor              = RedisNew("invalid cursor")
	NotFoundCursor             = RedisNew("not found cursor")
	InvalidChannel             = RedisNew("invalid channel name")
	InvalidPartition           = RedisNew("invalid partition")
	ErrOutOfRange              = RedisNew("index out of range")
	ErrDBIndexOutOfRange       = RedisNew("DB index is out of range")
	ErrOverflow                = RedisNew("increment or decrement would overflow")
	ErrNotFound                = RedisNew("value not found")
	TimeOut                    = RedisNew("time out")
	ErrXADDID                  = RedisNew("The ID specified in XADD is equal or smaller than the target stream top item")
	InvalidStreamID            = RedisNew("Invalid stream ID specified as stream command argument")
	XgroupRequireExist         = RedisNew("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	XgroupAlreadyExist         = RedisNew("BUSYGROUP Consumer Group name already exists")
	ErrStartGreaterThanEnd     = RedisNew("start is larger than the end of the list")
	ErrNoSuchKey               = RedisNew("no such key")
	InvalidWeight              = RedisNew("weight value is not a float")
	ErrClientClosed            = RedisNew("client closed")
	ErrUnBlocked               = RedisNew("UNBLOCKED client unblocked via CLIENT UNBLOCK")
	ErrNoSuchMaster            = RedisNew("No such master with that name")
	ErrGeoFromSpecified        = RedisNew("exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch")
	ErrGeoBySpecified          = RedisNew("exactly one of BYRADIUS and BYBOX can be specified for geosearch")

	InvalidOffset     = RedisNew("invalid offset")
	InvalidLimit      = RedisNew("invalid limit")
	ErrClosed         = RedisNew("closed")
	ErrEmpty          = RedisNew("emtpy")
	ErrInternalServer = RedisNew("internal server error")
)

func WrongArgsString(command string) string {
	return fmt.Sprintf("wrong number of arguments for '%s' command", command)
}

func NotExistKey(key string) string {
	return fmt.Sprintf("key '%s' does not exist in path", key)
}

func UnsupportedOption(opt []byte) string {
	return fmt.Sprintf("Unsupported option %s", opt)
}

func UnknownCommand(command string) string {
	return fmt.Sprintf("unknown command `%s`, with args beginning with:", command)

}

func InvalidExpire(command string) string {
	return fmt.Sprintf("invalid expire time in %s", command)
}

func InvalidCommand(command string) string {
	return fmt.Sprintf("Can't execute '%s': "+
		"only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are "+
		"allowed in this context", command)
}

func WrongModifier(command, modifier string, err error) error {
	return fmt.Errorf("Error in %s modifier '%s': %s", strings.ToUpper(command), modifier, err.Error())
}

func NotExistKeyError(key string) error {
	return RedisNew(NotExistKey(key))
}

func UnsupportedOptionError(opt []byte) error {
	return RedisNew(UnsupportedOption(opt))
}

func WrongArgsError(command string) error {
	return RedisNew(WrongArgsString(command))
}

func UnknownCommandError(command string) error {
	return RedisNew(UnknownCommand(command))
}

func InvalidExpireError(command string) error {
	return RedisNew(InvalidExpire(command))
}

func InvalidCommandError(command string) error {
	return RedisNew(InvalidCommand(command))
}

func WrongSubArgsString(command, help string) string {
	return fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'. Try %s.",
		command, help)
}

func WrongSubArgsError(command, help string) error {
	return RedisNew(WrongSubArgsString(command, help))
}

func WrongPermissionString(command string) string {
	return fmt.Sprintf("this user has no permissions to run the '%s' command or its subcommand",
		command)
}

func InvalidGEO(longitude, latitude float64) string {
	return fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", longitude, latitude)
}

func InvalidGEOError(longitude, latitude float64) error {
	return RedisNew(InvalidGEO(longitude, latitude))
}

func WrongPermissionError(command string) error {
	return RedisNew(WrongPermissionString(command))
}

func WrongUsernameString(username string) string {
	return fmt.Sprintf("invalid username '%s'", username)
}

func WrongUsernameError(username string) error {
	return RedisNew(WrongUsernameString(username))
}

func MakeSafeErr(err error) error {
	msg := MakeSafeString(err.Error())
	return RedisNew(msg)
}

func MakeSafeString(msg string) string {
	return strings.Replace(msg, "\n", ` `, -1)
}

func MakeSafe(err string) error {
	return errors.New(MakeSafeString(err))
}
