package command

import "gitlab.s.upyun.com/platform/lancelot/redcon"

const (
	KEYSPACE_CATEGORY    = "keyspace"
	READ_CATEGORY        = "read"
	WRITE_CATEGORY       = "write"
	SET_CATEGORY         = "set"
	SORTEDSET_CATEGORY   = "sortedset"
	LIST_CATEGORY        = "list"
	HASH_CATEGORY        = "hash"
	STRING_CATEGORY      = "string"
	BITMAP_CATEGORY      = "bitmap"
	HYPERLOGLOG_CATEGORY = "hyperloglog"
	GEO_CATEGORY         = "geo"
	STREAM_CATEGORY      = "stream"
	PUBSUB_CATEGORY      = "pubsub"
	AMDIN_CATEGORY       = "admin"
	FAST_CATEGORY        = "fast"
	SLOW_CATEGORY        = "slow"
	BLOCKING_CATEGORY    = "blocking"
	DANGEROUS_CATEGORY   = "dangerous"
	CONNECTION_CATEGORY  = "connection"
	TRANSCATION_CATEGORY = "transaction"
	SCRIPTING_CATEGORY   = "scripting"

	JSONSET_COMMAND    = "json.set"
	JSONGET_COMMAND    = "json.get"
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
	AUTH_COMMAND       = "auth"
	ACL_COMMAND        = "acl"
	CAT_COMMAND        = "cat"
	DELUSER_COMMAND    = "deluser"
	GENPASS_COMMAND    = "genpass"
	GETUSER_COMMAND    = "getuser"
	SETUSER_COMMAND    = "setuser"
	USERS_COMMAND      = "users"
	LIST_COMMAND       = "list"
	SAVE_COMMAND       = "save"
	PING_COMMAND       = "ping"
	SHUTDONW_COMMAND   = "shutdown"
	WATCH_COMMAND      = "watch"
	EXEC_COMMAND       = "exec"
	MULTI_COMMAND      = "multi"
	DISCARD_COMMAND    = "discard"

	MAX_COMMANDS = 1024

	ASYNC_OPTION = "async"
	SYNC_OPTION  = "sync"

	OK     = redcon.SimpleString("OK")
	Queued = redcon.SimpleString("QUEUED")
	PONG   = redcon.SimpleString("PONG")
)

var (
	CommandObjectTypes = map[string]ObjectType{
		JSONSET_COMMAND: JsonType,
		JSONGET_COMMAND: JsonType,
		HSET_COMMAND:    HashType,
		HGET_COMMAND:    HashType,
		SET_COMMAND:     KeyType,
		GET_COMMAND:     KeyType,
	}
)

func SimpleInt(n int) redcon.SimpleInt {
	return redcon.SimpleInt(n)
}

func SimpleString(n string) redcon.SimpleString {
	return redcon.SimpleString(n)
}
