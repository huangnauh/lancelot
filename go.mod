module gitlab.s.upyun.com/platform/lancelot

go 1.16

require (
	github.com/cloudflare/tableflip v1.2.2
	github.com/google/uuid v1.1.1
	github.com/pingcap/tidb v1.1.0-beta.0.20201222032702-32d8cad845d6
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3 // indirect
	github.com/tidwall/redcon v1.4.2-0.20210420214626-0cb26bc5a4b7
	github.com/vmihailenco/msgpack/v5 v5.3.4
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22 // indirect
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/tidwall/redcon v1.4.2-0.20210420214626-0cb26bc5a4b7 => github.com/huangnauh/redcon v1.4.2-0.20210709065658-d01c457d0367
