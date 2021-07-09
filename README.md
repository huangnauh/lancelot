## lancelot

### build

`go build ./cmd/redis-server`


### run

`./redis-server`

### support

- [x] (string) GET key
- [x] (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL [NX|XX] [GET]
- [x] (string) TTL key
- [x] (transactions) WATCH key
- [x] (transactions) MULTI
- [x] (transactions) EXEC