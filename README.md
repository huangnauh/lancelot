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