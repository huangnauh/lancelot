package command

import "gitlab.s.upyun.com/platform/lancelot/redcon"

const (
	HSET_COMMAND       = "hset"
	HGET_COMMAND       = "hget"
	SET_COMMAND        = "set"
	GET_COMMAND        = "get"
	DEL_COMMAND        = "del"
	TTL_COMMAND        = "ttl"
	OBJECT_COMMAND     = "object"
	HELP_COMMAND       = "help"
	ENCODING_COMMAND   = "encoding"
	FREQ_COMMAND       = "freq"
	IDLETIME_COMMAND   = "idletime"
	REFCOUNT_COMMAND   = "refcount"
	EVAL_COMMAND       = "eval"
	EVALSHA_COMMAND    = "evalsha"
	EVAL_RO_COMMAND    = "eval_ro"
	EVALSHA_RO_COMMAND = "evalsha_ro"
	SCRIPT_COMMAND     = "script"
	LOAD_COMMAND       = "load"
	FLUSH_COMMAND      = "flush"
	EXISTS_COMMAND     = "exists"
	KILL_COMMAND       = "kill"

	ASYNC_OPTION = "async"
	SYNC_OPTION  = "sync"

	OK     = redcon.SimpleString("OK")
	Queued = redcon.SimpleString("QUEUED")
	PONG   = redcon.SimpleString("PONG")
)

func SimpleInt(n int) redcon.SimpleInt {
	return redcon.SimpleInt(n)
}

func SimpleString(n string) redcon.SimpleString {
	return redcon.SimpleString(n)
}
