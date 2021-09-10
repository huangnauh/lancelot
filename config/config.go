package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

type Store struct {
	Path               string        `yaml:"path"`
	Level              string        `yaml:"level"`
	GCEnable           bool          `yaml:"gc-enable"`
	SlowRequest        time.Duration `yaml:"slow-request"`
	ReadTimeout        time.Duration `yaml:"read-timeout"`
	ListTimeout        time.Duration `yaml:"list-timeout"`
	WriteTimeout       time.Duration `yaml:"write-timeout"`
	BatchPutTimeout    time.Duration `yaml:"batch-put-timeout"`
	BatchDeleteTimeout time.Duration `yaml:"batch-delete-timeout"`
	TsoSlowThreshold   time.Duration `yaml:"tso-slow-threshold"`
	DisableLockBackOff bool          `yaml:"disable-lock-back-off"`
	BatchLimit         int           `yaml:"batch-limit"`
}

type Lua struct {
	InitPoolSize int `yaml:"init-pool-size"`
	MaxPoolSize  int `yaml:"max-pool-size"`
}

type Auth struct {
	Root                string `yaml:"root"`
	Pass                string `yaml:"pass"`
	MaxUsers            int    `yaml:"max-users"`
	MaxPasswordsPerUser int    `yaml:"max-passwords-per-user"`
	MaxDBPerUser        uint8  `yaml:"max-db-per-user"`
}

type Key struct {
	ScanMaxCount       int    `yaml:"scan-max-count"`
	CursorExpireSecond int    `yaml:"cursor-expirate-second"`
	DbSizeHash         uint64 `yaml:"db-size-hash"`
}

type Rpc struct {
	Timeout    time.Duration `yaml:"timeout"`
	MaxStream  uint32        `yaml:"max_stream"`
	MaxMsgSize int           `yaml:"max_msg_size"`
	Compress   bool          `yaml:"compress"`
}

type GC struct {
	TickInterval  time.Duration `yaml:"tick-interval"`
	TTLWorkers    int           `yaml:"ttl-workers"`
	TTLBatchLimit int           `yaml:"ttl-batch-limit"`
}

type PubSub struct {
	MaxSlowMessagePerSubscribe int `yaml:"max-slow-message-per-subscribe"`
}

type Config struct {
	StartAt   time.Time `yaml:"-"`
	LogLevel  string    `yaml:"log-level"`
	PIDFile   string    `yaml:"pid-file"`
	Host      string    `yaml:"host"`
	RedisPort int       `yaml:"redis-port"`
	HttpPort  int       `yaml:"http-port"`
	RpcPort   int       `yaml:"rpc-port"`
	CacheSize int       `yaml:"cache-size"`
	Store     Store     `yaml:"store"`
	GC        GC        `yaml:"gc"`
	Lua       Lua       `yaml:"lua"`
	Auth      Auth      `yaml:"auth"`
	Key       Key       `yaml:"key"`
	Rpc       Rpc       `yaml:"rpc"`
	PubSub    PubSub    `yaml:"pubsub"`
}

var cfg = &Config{
	LogLevel:  "debug",
	PIDFile:   "redis.pid",
	Host:      "127.0.0.1",
	RedisPort: 6379,
	HttpPort:  6380,
	RpcPort:   6381,
	CacheSize: 10 * 1024 * 1024,
	Store: Store{
		Path:               "mocktikv://",
		Level:              "debug",
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
		InitPoolSize: 10,
		MaxPoolSize:  100,
	},
	Auth: Auth{
		Root:                "root",
		Pass:                "root",
		MaxUsers:            1000,
		MaxPasswordsPerUser: 10,
		MaxDBPerUser:        64,
	},
	Key: Key{
		ScanMaxCount:       10000,
		CursorExpireSecond: 10 * 60,
	},
	Rpc: Rpc{
		Timeout:    time.Second * 10,
		MaxStream:  1000,
		MaxMsgSize: 1024 * 1024,
	},
	PubSub: PubSub{
		MaxSlowMessagePerSubscribe: 1000,
	},
}

func LoadYAMLConfig(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("ReadFile: %v", err)
	}
	err = yaml.Unmarshal(data, cfg)
	cfg.StartAt = time.Now()
	return err
}

func GetConfig() *Config {
	return cfg
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
