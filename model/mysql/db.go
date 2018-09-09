package mysql

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/usthooz/gutil"
	"github.com/usthooz/oozkits/model/redis"
)

// DB mysql db and cache redi.
type DB struct {
	*sqlx.DB
	Cache     *redis.Client
	dbConfig  *Config
	rdsConfig *redis.Config
	cacheDBs  map[string]*CacheDB
}

// Connect connection db.
func Connect(dbConfig *Config, redisConfig *redis.Config) (*DB, error) {
	var (
		cache *redis.Client
	)
	// open db cache.
	if !dbConfig.OpenCache || redisConfig != nil {
		var (
			err error
		)
		if cache, err = redis.NewClient(redisConfig); err != nil {
			// new client err
			// ozlog.Errorf("Db Connect: new client err.")
			return nil, err
		}
	}
	// open db
	db, err := sqlx.Open("mysql", dbConfig.Source())
	if err != nil {
		return nil, err
	}
	// max is conns
	db.SetMaxIdleConns(dbConfig.MaxIdleConns)
	// max open count
	db.SetMaxOpenConns(dbConfig.MaxOpenConns)
	// time: s
	db.SetConnMaxLifetime(time.Duration(dbConfig.ConnMaxLifetime) * time.Second)
	db.Mapper = reflectx.NewMapperFunc("json", gutil.FieldSnakeString)
	return &DB{
		DB:        db,
		dbConfig:  dbConfig,
		Cache:     cache,
		rdsConfig: redisConfig,
		cacheDBs:  make(map[string]*CacheDB),
	}, nil
}

// CacheDB
type CacheDB struct {
	*DB
	tableName         string
	cachePriKeyPrefix string
	cols              []string
	priCols           []string
	cacheExpiration   time.Duration
	typeName          string
	priFieldsIndex    []int          // primary column index in struct
	fieldsIndexMap    map[string]int // key:colName, value:field index in struct
	module            *redis.Module
}
