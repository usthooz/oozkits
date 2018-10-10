package mongo

import (
	"fmt"
	"time"

	"github.com/usthooz/oozkits/model/redis"
	"gopkg.in/mgo.v2"
)

// PreDB preset *DB
type PreDB struct {
	*DB
	preFuncs map[string]func() error
	inited   bool
}

// NewPreDB
func NewPreDB() *PreDB {
	return &PreDB{
		DB: &DB{
			cacheDBs: make(map[string]*CacheDB),
		},
		preFuncs: make(map[string]func() error),
	}
}

func (p *PreDB) Init(dbConfig *Config, redisConfig *redis.Config) (err error) {
	var (
		cache *redis.Client
	)
	if !dbConfig.CloseCache && redisConfig != nil {
		cache, err = redis.NewClient(redisConfig)
		if err != nil {
			return err
		}
	}
	return p.InitByCache(dbConfig, cache)
}

// InitByCache
func (p *PreDB) InitByCache(dbConfig *Config, redisClient *redis.Client) (err error) {
	// connect
	db, err := mgo.DialWithInfo(dbConfig.Source())
	if err != nil {
		return err
	}
	p.DB.Session = db
	p.DB.dbConfig = dbConfig
	if !dbConfig.CloseCache && redisClient != nil {
		p.DB.Cache = redisClient
		p.DB.rdsCondif = redisClient.GetConfig()
	}
	for _, preFunc := range p.preFuncs {
		if err = preFunc(); err != nil {
			return err
		}
	}
	p.inited = true
	return nil
}

// RegisterCacheableDB
func (p *PreDB) RegisterCacheableDB(ormStructPtr Cacheable, cacheExpire time.Duration) (*CacheDB, error) {
	if p.inited {
		return p.DB.RegisterCacheDB(ormStructPtr, cacheExpire)
	}

	tableName := ormStructPtr.TableName()
	if _, ok := p.preFuncs[ormStructPtr.TableName()]; ok {
		return nil, fmt.Errorf("already register cacheable table is: %s", tableName)
	}
	var (
		cacheableDB = new(CacheDB)
		preFunc     = func() error {
			_cacheableDB, err := p.DB.RegisterCacheDB(ormStructPtr, cacheExpire)
			if err == nil {
				*cacheableDB = *_cacheableDB
				p.DB.cacheDBs[tableName] = cacheableDB
			}
			return err
		}
	)
	p.preFuncs[tableName] = preFunc
	return cacheableDB, nil
}
