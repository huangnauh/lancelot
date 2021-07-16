## lancelot

### build

`go build ./cmd/redis-server`


### run

`./redis-server`

### support

- [x] (string) GET key
- [x] (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL [NX|XX] [GET]
- [x] (hash) HSET key field value
- [x] (hash) HGET key field
- [x] (transactions) WATCH key
- [x] (transactions) MULTI
- [x] (transactions) EXEC
- [x] (generic) OBJECT subcommand [arguments [arguments ...]]
- [x] (generic) TTL key
- [x] (generic) DEL key [key ...]
- [x] (scripting) EVAL script numkeys [key [key ...]] [arg [arg ...]]
- [x] (scripting) EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
- [x] (scripting) EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
- [x] (scripting) EVAL_RO script numkeys key [key ...] arg [arg ...]

#### Available libraries
- [x] redis.call function.
- [x] redis.pcall function.
- [x] redis.error_reply function.
- [x] redis.status_reply function.
- [x] redis.sha1hex function.
- [x] json/cjson lib.
