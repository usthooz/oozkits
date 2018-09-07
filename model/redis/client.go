// Based on `github.com/go-redis/redis`v6.6.0
package redis

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// redis deploy type
const (
	DeploySingle  string = "single"
	DeployCluster string = "cluster"
)

type (
	// Config redis and cluster client config
	Config struct {
		// redis deploy type, [single, cluster]
		DeployType string `yaml:"deploy_type"`
		// only for single node config, valid when DeployType=single.
		ForSingle SingleConfig `yaml:"for_single"`
		// only for cluster config, valid when DeployType=cluster.
		ForCluster ClusterConfig `yaml:"for_cluster"`
		// An optional password. Must match the password specified in the
		// requirepass server configuration option.
		Password string `yaml:"password,omitempty"`
		// The maximum number of retries before giving up.
		// Default is to not retry failed commands.
		MaxRetries int `yaml:"max_retries,omitempty"`
		// Dial timeout for establishing new connections.
		// Default is 5 seconds.
		DialTimeout int64 `yaml:"dial_timeout,omitempty"`
		// Timeout for socket reads. If reached, commands will fail
		// with a timeout instead of blocking.
		// Default is 3 seconds.
		ReadTimeout int64 `yaml:"read_timeout,omitempty"`
		// Timeout for socket writes. If reached, commands will fail
		// with a timeout instead of blocking.
		// Default is ReadTimeout.
		WriteTimeout int64 `yaml:"write_timeout,omitempty"`
		// PoolSizePerNode applies per cluster node and not for the whole cluster.
		// Maximum number of socket connections.
		// Default is 10 connections per every CPU as reported by runtime.NumCPU.
		PoolSizePerNode int `yaml:"pool_size_per_node"`
		// Amount of time client waits for connection if all connections
		// are busy before returning an error.
		// Default is ReadTimeout + 1 second.
		PoolTimeout int64 `yaml:"pool_timeout,omitempty"`
		// Amount of time after which client closes idle connections.
		// Should be less than server's timeout.
		// Default is 300 seconds.
		IdleTimeout int64 `yaml:"idle_timeout"`
		// Frequency of idle checks.
		// Default is 60 seconds.
		// When minus value is set, then idle check is disabled.
		IdleCheckFrequency int64 `yaml:"idle_check_frequency,omitempty"`
		// Enables read only queries on slave nodes.
		// Only for cluster.
		ReadOnly bool `yaml:"read_only,omitempty"`
	}
	// SingleConfig redis single node client config.
	SingleConfig struct {
		// host:port and address.
		Addr string `yaml:"addr"`
		// Maximum backoff between each retry.
		// Default is 512 seconds; -1 disables backoff.
		MaxRetryBackoff int64 `yaml:"max_retry_backoff,omitempty"`
	}
	// ClusterConfig redis cluster client config.
	ClusterConfig struct {
		// A seed list of host:port addresses of cluster nodes.
		Addrs []string `yaml:"addrs"`
		// The maximum number of retries before giving up. Command is retried
		// on network errors and MOVED/ASK redirects.
		// Default is 16.
		MaxRedirects int `yaml:"max_redirects,omitempty"`
		// Enables routing read-only queries to the closest master or slave node.
		RouteByLatency bool `yaml:"route_by_latency,omitempty"`
	}
)

// Client redis client and cluster client merge.
type (
	Client struct {
		cfg *Config
		Cmdable
	}
	Cmdable interface {
		redis.Cmdable
		TxPipeline() redis.Pipeliner
		TxPipelined(fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
		Publish(channel, message string) *redis.IntCmd
		Subscribe(channels ...string) *redis.PubSub
	}
	// Alias-> usth ooz.redis's method copy to go-redis.redis
	PubSub             = redis.PubSub
	Message            = redis.Message
	GeoLocation        = redis.GeoLocation
	GeoRadiusQuery     = redis.GeoRadiusQuery
	ZRangeBy           = redis.ZRangeBy
	Z                  = redis.Z
	Pipeliner          = redis.Pipeliner
	RedisCmdable       = redis.Cmdable
	SliceCmd           = redis.SliceCmd
	StatusCmd          = redis.StatusCmd
	Cmder              = redis.Cmder
	IntCmd             = redis.IntCmd
	DurationCmd        = redis.DurationCmd
	BoolCmd            = redis.BoolCmd
	StringCmd          = redis.StringCmd
	FloatCmd           = redis.FloatCmd
	StringSliceCmd     = redis.StringSliceCmd
	BoolSliceCmd       = redis.BoolSliceCmd
	StringStringMapCmd = redis.StringStringMapCmd
	StringIntMapCmd    = redis.StringIntMapCmd
	ZSliceCmd          = redis.ZSliceCmd
	ScanCmd            = redis.ScanCmd
	ClusterSlotsCmd    = redis.ClusterSlotsCmd
)

// NewClient new redis client and cluster redis.
func NewClient(cfg *Config) (*Client, error) {
	var (
		c = &Client{
			cfg: cfg,
		}
	)
	switch cfg.DeployType {
	case DeploySingle:
		// redis client
		c.Cmdable = redis.NewClient(&redis.Options{
			Addr:               cfg.ForSingle.Addr,
			Password:           cfg.Password,
			MaxRetries:         cfg.MaxRetries,
			PoolSize:           cfg.PoolSizePerNode,
			MaxRetryBackoff:    time.Duration(cfg.ForSingle.MaxRetryBackoff) * time.Second,
			DialTimeout:        time.Duration(cfg.DialTimeout) * time.Second,
			ReadTimeout:        time.Duration(cfg.ReadTimeout) * time.Second,
			WriteTimeout:       time.Duration(cfg.WriteTimeout) * time.Second,
			PoolTimeout:        time.Duration(cfg.PoolTimeout) * time.Second,
			IdleTimeout:        time.Duration(cfg.IdleTimeout) * time.Second,
			IdleCheckFrequency: time.Duration(cfg.IdleCheckFrequency) * time.Second,
		})
	case DeployCluster:
		// redis cluster client
		c.Cmdable = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:              cfg.ForCluster.Addrs,
			Password:           cfg.Password,
			ReadOnly:           cfg.ReadOnly,
			MaxRetries:         cfg.MaxRetries,
			PoolSize:           cfg.PoolSizePerNode,
			MaxRedirects:       cfg.ForCluster.MaxRedirects,
			RouteByLatency:     cfg.ForCluster.RouteByLatency,
			DialTimeout:        time.Duration(cfg.DialTimeout) * time.Second,
			ReadTimeout:        time.Duration(cfg.ReadTimeout) * time.Second,
			WriteTimeout:       time.Duration(cfg.WriteTimeout) * time.Second,
			PoolTimeout:        time.Duration(cfg.PoolTimeout) * time.Second,
			IdleTimeout:        time.Duration(cfg.IdleTimeout) * time.Second,
			IdleCheckFrequency: time.Duration(cfg.IdleCheckFrequency) * time.Second,
		})
	default:
		return nil, fmt.Errorf("Config.DeployType: optionals-> %s, %s, this cfg cat't nil.", DeploySingle, DeployCluster)
	}
	if _, err := c.Ping().Result(); err != nil {
		return nil, err
	}
	return c, nil
}
