package mysql

import (
	"fmt"
	"time"

	"github.com/usthooz/gutil"
	"github.com/usthooz/oozkits/model/redis"
	"github.com/usthooz/sqlx"
	"github.com/usthooz/sqlx/reflectx"
)

// PreDB
type PreDB struct {
	*DB
	preFuncs map[string]func() error
	inited   bool
}

// NewPreDB
func NewPreDB() *PreDB {
	return &PreDB{
		DB: &DB{
			// cache db
			cacheDBs: make(map[string]*CacheDB),
		},
		preFuncs: make(map[string]func() error),
	}
}

// InitByRdsCfg
func (p *PreDB) InitByRdsCfg(dbConfig *Config, redisConfig *redis.Config) (err error) {
	var (
		cacheClient *redis.Client
	)
	if !dbConfig.CloseCache && redisConfig != nil {
		cacheClient, err = redis.NewClient(redisConfig)
		if err != nil {
			return err
		}
	}
	return p.InitByRdsClient(dbConfig, cacheClient)
}

// InitByRdsClient
func (p *PreDB) InitByRdsClient(dbConfig *Config, redisCient *redis.Client) (err error) {
	p.DB.DB, err = sqlx.Connect("mysql", dbConfig.Source())
	if err != nil {
		return err
	}
	p.DB.SetMaxOpenConns(dbConfig.MaxOpenConns)
	p.DB.SetMaxIdleConns(dbConfig.MaxIdleConns)
	p.DB.SetConnMaxLifetime(time.Duration(dbConfig.ConnMaxLifetime) * time.Second)
	p.DB.Mapper = reflectx.NewMapperFunc("json", gutil.FieldSnakeString)
	p.DB.dbConfig = dbConfig
	if !dbConfig.CloseCache && redisCient != nil {
		p.DB.Cache = redisCient
		p.DB.rdsConfig = redisCient.GetConfig()
	}
	for _, preFunc := range p.preFuncs {
		if err = preFunc(); err != nil {
			return err
		}
	}
	p.inited = true
	return nil
}

// RegisterCacheDB
func (p *PreDB) RegisterCacheDB(ormStructPtr Cacheable, cacheExpiration time.Duration, initQuery string, args ...interface{}) (*CacheDB, error) {
	if p.inited {
		if len(initQuery) > 0 {
			_, err := p.DB.Exec(initQuery, args...)
			if err != nil {
				return nil, err
			}
		}
		return p.DB.RegisterCacheDB(ormStructPtr, cacheExpiration)
	}
	tableName := ormStructPtr.TableName()
	if _, ok := p.preFuncs[ormStructPtr.TableName()]; ok {
		return nil, fmt.Errorf("already register cache: %s", tableName)
	}
	var (
		cacheableDB = new(CacheDB)
	)
	var preFunc = func() error {
		if len(initQuery) > 0 {
			_, err := p.DB.Exec(initQuery, args...)
			if err != nil {
				return err
			}
		}
		_cacheableDB, err := p.DB.RegisterCacheDB(ormStructPtr, cacheExpiration)
		if err == nil {
			*cacheableDB = *_cacheableDB
			p.DB.cacheDBs[tableName] = cacheableDB
		}
		return err
	}
	p.preFuncs[tableName] = preFunc
	return cacheableDB, nil
}
