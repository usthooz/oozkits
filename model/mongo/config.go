package mongo

import (
	"time"

	"gopkg.in/mgo.v2"
)

// Config mongodb config
type Config struct {
	// Addrs host
	Addrs []string `yaml:"addr"`
	// Timeout
	Timeout time.Duration `yaml:"timeout"`
	// Database mgo db name
	Database string `yaml:"database"`
	// Username
	Username string `yaml:"username"`
	// Password
	Password string `yaml:"passward"`
	// PoolLimit
	PoolLimit int `yaml:"pool_limit"`
	// CloseCache default open cache
	CloseCache bool `yaml:"close_cache"`
}

// NewConfig
func NewConfig() *Config {
	return &Config{
		Addrs:     []string{"127.0.0.1:27017"},
		Timeout:   10,
		Database:  "test",
		Username:  "root",
		PoolLimit: 256,
	}
}

// Source
func (mgoConfig *Config) Source() *mgo.DialInfo {
	dialInfo := &mgo.DialInfo{
		Addrs:     mgoConfig.Addrs,
		Username:  mgoConfig.Username,
		Password:  mgoConfig.Password,
		Database:  mgoConfig.Database,
		Timeout:   mgoConfig.Timeout,
		PoolLimit: mgoConfig.PoolLimit,
	}
	return dialInfo
}
