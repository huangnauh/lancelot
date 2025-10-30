module github.com/huangnauh/lancelot

go 1.16

require (
	github.com/alicebob/miniredis/v2 v2.16.0
	github.com/axiomhq/hyperloglog v0.0.0-20220105174342-98591331716a
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/cespare/xxhash v1.1.0
	github.com/cloudflare/tableflip v1.2.2
	github.com/coocood/freecache v1.1.1
	github.com/go-redis/redis/v8 v8.11.4
	github.com/gocraft/work v0.5.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551
	github.com/golang/protobuf v1.5.2
	github.com/gomodule/redigo v1.8.5
	github.com/google/uuid v1.1.2
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/json-iterator/go v1.1.12
	github.com/mmcloughlin/geohash v0.10.1-0.20210831075534-dc9a53a52fad
	github.com/nitishm/go-rejson/v4 v4.1.0
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.16.0
	github.com/opentracing/basictracer-go v1.1.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pingcap/errors v0.11.5-0.20210513014640-40f9a1999b3b
	github.com/pingcap/kvproto v0.0.0-20211011042309-a4518fcacbc8
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v1.1.0-beta.0.20211025024448-36e694bfc536
	github.com/pingcap/tidb/parser v0.0.0-20211025024448-36e694bfc536 // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/robfig/cron v1.2.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.2.1
	github.com/spyzhov/ajson v0.7.1
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/btree v0.5.0
	github.com/tidwall/gjson v1.8.1 // indirect
	github.com/tidwall/match v1.0.3
	github.com/tidwall/sjson v1.1.7
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20211011083157-49c8dd23f1f0
	github.com/valyala/fastjson v1.6.3
	github.com/vmihailenco/msgpack/v5 v5.3.4
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/goleak v1.1.11-0.20210813005559-691160354723
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/grpc v1.40.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20211028080628-e2786a622600
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67
)

replace (
	github.com/go-redis/redis/v8 v8.11.4 => github.com/huangnauh/redis/v8 v8.11.5-0.20211029022615-7f6b8adada1c
	github.com/spyzhov/ajson v0.7.1 => github.com/huangnauh/ajson v0.7.2-b
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20211011083157-49c8dd23f1f0 => github.com/huangnauh/client-go/v2 v2.0.0-alpha.0.20220304080027-748d059ebe31
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9 => github.com/huangnauh/gopher-lua v0.0.0-20210930062039-32ec5e06c52a
	google.golang.org/grpc => google.golang.org/grpc v1.29.1
)
