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
type Config struct {
	LogLevel       string        `yaml:"log-level"`
	PIDFile        string        `yaml:"pid-file"`
	Host           string        `yaml:"host"`
	RedisPort      int           `yaml:"redis-port"`
	HttpPort       int           `yaml:"http-port"`
	Store          Store         `yaml:"store"`
	GcTickInterval time.Duration `yaml:"gc-tick-interval"`
	GcWorkers      int           `yaml:"gc-workers"`
	Lua            Lua           `yaml:"lua"`
}

var cfg = &Config{
	LogLevel:  "debug",
	PIDFile:   "redis.pid",
	Host:      "0.0.0.0",
	RedisPort: 6379,
	HttpPort:  6380,
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
	GcTickInterval: time.Minute,
	GcWorkers:      2,
	Lua: Lua{
		InitPoolSize: 10,
		MaxPoolSize:  100,
	},
}

func LoadYAMLConfig(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("ReadFile: %v", err)
	}
	err = yaml.Unmarshal(data, cfg)
	return err
}

func GetConfig() *Config {
	return cfg
}

func SaveConfig(savePath string) {
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
