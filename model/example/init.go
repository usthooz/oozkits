package example

import (
	"time"

	"github.com/usthooz/oozkits/model/mysql"
	"github.com/usthooz/oozkits/model/redis"
)

var (
	// mysqlHandler preset mysql DB handler
	mysqlHandler = mysql.NewPreDB()
	// redisClient redis cache and db client
	redisClient *redis.Client
	// cacheExpire cache time duration
	cacheExpire time.Duration
)

// Init
func Init(mysqlConfig *mysql.Config, redisConfig *redis.Config, _cacheExpire time.Duration) error {
	var (
		err error
	)
	cacheExpire = _cacheExpire
	// redis
	if redisConfig != nil {
		if redisClient, err = redis.NewClient(redisConfig); err != nil {
			return err
		}
	}
	// mysql and cache db
	if mysqlConfig != nil {
		if err = mysqlHandler.InitByRdsClient(mysqlConfig, redisClient); err != nil {
			return err
		}
	}
	return nil
}

// GetMysqlDB get mysql handler.
func GetMysqlDB() *mysql.DB {
	return mysqlHandler.DB
}

// GetRedis get redis client.
func GetRedis() *redis.Client {
	return redisClient
}
