## lancelot

### build

`go build ./cmd/redis-server`

### run

`./redis-server`

### test
`make test`

### support

- [x] (string) GET key
- [x] (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL [NX|XX] [GET]
- [x] (hash) HSET key field value [field value ...]
- [x] (hash) HGET key field
- [x] (json) JSON.SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL [NX|XX] [GET]
- [x] (json) JSON.GET key [path [path ...]]
- [x] (json) JSON.DEL key path [path ...]
- [x] (server) ACL GETUSER username
- [x] (server) ACL SETUSER username [rule [rule ...]]
- [x] (server) ACL LIST
- [x] (connection) AUTH [username] password
- [x] (transactions) WATCH key
- [x] (transactions) MULTI
- [x] (transactions) EXEC
- [x] (generic) OBJECT subcommand [arguments [arguments ...]]
- [x] (generic) TTL key
- [x] (generic) DEL key [key ...]
- [x] (generic) SCAN cursor [MATCH pattern] [COUNT count] [TYPE type] [CURSOR cursor]
- [x] (scripting) EVAL script numkeys [key [key ...]] [arg [arg ...]]
- [x] (scripting) EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
- [x] (scripting) EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
- [x] (scripting) EVAL_RO script numkeys key [key ...] arg [arg ...]
- [x] (scripting) SCRIPT LOAD script
- [x] (scripting) SCRIPT FLUSH [ASYNC|SYNC]
- [x] (scripting) SCRIPT EXISTS sha1 [sha1 ...]

#### Available libraries
- [x] redis.call function.
- [x] redis.pcall function.
- [x] redis.error_reply function.
- [x] redis.status_reply function.
- [x] redis.sha1hex function.
- [x] cjson lib.
- [x] cmsgpack lib.
