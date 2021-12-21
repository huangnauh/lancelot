package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/json"

	"gopkg.in/yaml.v2"
)

const (
	ALIST = "a"
	BLIST = "b"
)

type Duration time.Duration

func (d Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d = Duration(value)
		return nil
	case string:
		dd, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		d = Duration(dd)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

type Store struct {
	PDAddrs            []string      `yaml:"pd-addrs" json:"-"`
	Level              string        `yaml:"level" json:"-"`
	UUID               string        `yaml:"uuid" json:"-"`
	GCEnable           bool          `yaml:"gc-enable" json:"-"`
	GCConcurrency      int           `yaml:"gc-concurrency" json:"gc-concurrency,omitempty"`
	SlowRequest        time.Duration `yaml:"slow-request" json:"slow-request,omitempty"`
	ReadTimeout        time.Duration `yaml:"read-timeout" json:"read-timeout,omitempty"`
	ListTimeout        time.Duration `yaml:"list-timeout" json:"list-timeout,omitempty"`
	WriteTimeout       time.Duration `yaml:"write-timeout" json:"write-timeout,omitempty"`
	BatchPutTimeout    time.Duration `yaml:"batch-put-timeout" json:"batch-put-timeout,omitempty"`
	BatchDeleteTimeout time.Duration `yaml:"batch-delete-timeout" json:"batch-delete-timeout,omitempty"`
	TsoSlowThreshold   time.Duration `yaml:"tso-slow-threshold" json:"tso-slow-threshold,omitempty"`
	DisableLockBackOff bool          `yaml:"disable-lock-back-off" json:"disable-lock-back-off,omitempty"`
	BatchLimit         int           `yaml:"batch-limit" json:"batch-limit,omitempty"`
}

type Lua struct {
	InitPoolSize int           `yaml:"init-pool-size" json:"init-pool-size,omitempty"`
	MaxPoolSize  int           `yaml:"max-pool-size" json:"max-pool-size,omitempty"`
	Timeout      time.Duration `yaml:"timeout" json:"timeout,omitempty"`
}

type Auth struct {
	Root                string `yaml:"root" json:"-"`
	Pass                string `yaml:"pass" json:"-"`
	MaxUsers            int    `yaml:"max-users" json:"max-users,omitempty"`
	MaxPasswordsPerUser int    `yaml:"max-passwords-per-user" json:"max-passwords-per-user,omitempty"`
	MaxDBPerUser        uint8  `yaml:"max-db-per-user" json:"max-db-per-user,omitempty"`
}

type Rpc struct {
	Timeout    time.Duration `yaml:"timeout" json:"timeout,omitempty"`
	MaxStream  uint32        `yaml:"max_stream" json:"max_stream,omitempty"`
	MaxMsgSize int           `yaml:"max_msg_size" json:"max_msg_size,omitempty"`
	Compress   bool          `yaml:"compress" json:"-"`
}

type GC struct {
	TickInterval  time.Duration `yaml:"tick-interval" json:"tick-interval,omitempty"`
	TTLWorkers    int           `yaml:"ttl-workers" json:"ttl-workers,omitempty"`
	TTLBatchLimit int           `yaml:"ttl-batch-limit" json:"ttl-batch-limit,omitempty"`
}
type Redis struct {
	ScanMaxCount               int    `yaml:"scan-max-count" json:"scan-max-count,omitempty"`
	DbSizeHash                 uint64 `yaml:"db-size-hash" json:"db-size-hash,omitempty"`
	ListType                   string `yaml:"list-type" json:"list-type,omitempty"`
	CursorExpireSecond         int    `yaml:"cursor-expirate-second" json:"cursor-expirate-second,omitempty"`
	MaxSlowMessagePerSubscribe int    `yaml:"max-slow-msg-per-sub" json:"max-slow-msg-per-sub,omitempty"`
}

type Config struct {
	StartAt       time.Time `yaml:"-" json:"-"`
	Version       int64     `yaml:"version" json:"version,omitempty"`
	LogLevel      string    `yaml:"log-level" json:"-"`
	PIDFile       string    `yaml:"pid-file" json:"-"`
	Host          string    `yaml:"host" json:"-"`
	RedisPort     int       `yaml:"redis-port" json:"-"`
	HttpPort      int       `yaml:"http-port" json:"-"`
	RpcPort       int       `yaml:"rpc-port" json:"-"`
	CacheSize     int       `yaml:"cache-size" json:"cache-size,omitempty"`
	AclPermission bool      `yaml:"acl-permission" json:"acl-permission,omitempty"`
	Store         Store     `yaml:"store" json:"store,omitempty"`
	GC            GC        `yaml:"gc" json:"gc,omitempty"`
	Lua           Lua       `yaml:"lua" json:"lua,omitempty"`
	Auth          Auth      `yaml:"auth" json:"auth,omitempty"`
	Rpc           Rpc       `yaml:"rpc" json:"rpc,omitempty"`
	Redis         Redis     `yaml:"redis" json:"redis,omitempty"`
}

func Hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

var cfgData []byte
var cfg = Config{
	LogLevel:  "debug",
	PIDFile:   "redis.pid",
	Host:      "127.0.0.1",
	RedisPort: 6379,
	HttpPort:  6380,
	RpcPort:   6381,
	CacheSize: 10 * 1024 * 1024,
	Store: Store{
		PDAddrs:            nil,
		Level:              "debug",
		UUID:               Hostname(),
		GCConcurrency:      1,
		SlowRequest:        100 * time.Millisecond,
		ReadTimeout:        time.Second,
		ListTimeout:        time.Minute,
		WriteTimeout:       time.Second,
		BatchPutTimeout:    time.Minute,
		BatchDeleteTimeout: time.Minute,
		TsoSlowThreshold:   100 * time.Millisecond,
		BatchLimit:         200,
	},
	GC: GC{
		TickInterval:  time.Minute,
		TTLWorkers:    5,
		TTLBatchLimit: 100,
	},
	Lua: Lua{
		InitPoolSize: 1,
		MaxPoolSize:  1,
		Timeout:      time.Minute,
	},
	Auth: Auth{
		Root:                "root",
		Pass:                "root",
		MaxUsers:            1000,
		MaxPasswordsPerUser: 10,
		MaxDBPerUser:        64,
	},
	Rpc: Rpc{
		Timeout:    time.Second * 10,
		MaxStream:  1000,
		MaxMsgSize: 1024 * 1024,
	},
	Redis: Redis{
		MaxSlowMessagePerSubscribe: 1000,
		ListType:                   ALIST,
		ScanMaxCount:               10000,
		CursorExpireSecond:         10 * 60,
	},
}

func LoadYAMLConfig(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("ReadFile: %v", err)
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return fmt.Errorf("Unmarshal: %v", err)
	}
	cfg.StartAt = time.Now()
	cfgData, err = json.Marshal(cfg)
	return err
}

func GetDefaultConfig() Config {
	return cfg
}

func GetDefaultConfigData() []byte {
	return cfgData
}

func SaveYAMLConfig(savePath string) {
	f, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	encode := yaml.NewEncoder(f)
	err = encode.Encode(cfg)
	if err != nil {
		panic(err)
	}
}
