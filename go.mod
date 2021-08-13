module gitlab.s.upyun.com/platform/lancelot

go 1.16

require (
	github.com/cloudflare/tableflip v1.2.2
	github.com/coocood/freecache v1.1.1
	github.com/go-redis/redis/v8 v8.11.2
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.5.2
	github.com/gomodule/redigo v1.8.5
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/nitishm/go-rejson/v4 v4.0.0
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.15.0
	github.com/pingcap/kvproto v0.0.0-20201215060142-f3dafca4c7fd
	github.com/pingcap/tidb v1.1.0-beta.0.20201222032702-32d8cad845d6
	github.com/spf13/cobra v1.1.3 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/btree v0.5.0
	github.com/tidwall/gjson v1.8.1
	github.com/tidwall/match v1.0.3
	github.com/tidwall/sjson v1.1.7
	github.com/valyala/fastjson v1.6.3
	github.com/vmihailenco/msgpack/v5 v5.3.4
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/goleak v1.1.10
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.18.1
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22 // indirect
	golang.org/x/tools v0.1.5 // indirect
	google.golang.org/grpc v1.27.1
	gopkg.in/yaml.v2 v2.4.0
)

replace (
	github.com/go-redis/redis/v8 v8.11.2 => ../redis/v8
	github.com/golang/protobuf => github.com/golang/protobuf v1.3.4
)
