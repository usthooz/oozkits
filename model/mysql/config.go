package mysql

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// Config ooz mysql db config
type Config struct {
	Database     string
	Username     string
	Password     string
	Host         string
	Port         int
	MaxIdleConns int `yaml:"max_idle_conns"`
	MaxOpenConns int `yaml:"max_open_conns"`
	// ConnMaxLifetime time: second
	ConnMaxLifetime int64 `yaml:"conn_max_lifetime"`
	// db redis cache
	CloseCache bool `yaml:"close_cache"`
}

// NewConfig creates db default config.
func NewConfig() *Config {
	return &Config{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "",
		Database: "ooz",
	}
}

// Source mysql connection source.
func (cfg *Config) Source() string {
	pwd := cfg.Password
	if pwd != "" {
		pwd = ":" + pwd
	}
	port := cfg.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local&interpolateParams=true", cfg.Username, pwd, cfg.Host, port, cfg.Database)
}
