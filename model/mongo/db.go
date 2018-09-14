package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/usthooz/gutil"
	"github.com/usthooz/oozkits/model/redis"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	ErrNotFound = mgo.ErrNotFound
	NewObjectId = bson.NewObjectId
)

// DB mongodb db info
type DB struct {
	*mgo.Session
	Cache     *redis.Client
	dbConfig  *Config
	rdsCondif *redis.Config
	cacheDBs  map[string]*CacheDB
}

// CacheDB cache DB handle
type CacheDB struct {
	*DB
	tableName         string
	cachePriKeyPrefix string
	cacheExpire       time.Duration
	typeName          string
	module            *redis.Module
}

// ErrCacheIsNil db cache(redis) is nil
var ErrCacheIsNil = errors.New("*DB.Cache (redis) is nil")

// Cacheable
type Cacheable interface {
	TableName() string
}

// RegisterCacheDB
func (d *DB) RegisterCacheDB(structPtr Cacheable, cacheExpire time.Duration) (*CacheDB, error) {
	// get table name
	tableName := structPtr.TableName()
	if _, exists := d.cacheDBs[tableName]; exists {
		return nil, fmt.Errorf("Already register mgo table cache,table->%s", tableName)
	}
	// if open cache and rds is nil
	if !d.dbConfig.CloseCache && d.Cache == nil {
		return nil, ErrCacheIsNil
	}
	// new rds module
	module := redis.NewModule(d.dbConfig.Database + ":" + tableName)
	tpl := reflect.TypeOf(structPtr)
	c := &Cacheable{
		DB:          d,
		tableName:   tableName,
		cacheExpire: module.GetKey("_id"),
		cacheExpire: cacheExpire,
		typeName:    tpl.String(),
		module:      module,
	}
	// add
	d.cacheDBs[tableName] = c
	return c, nil
}

// GetCacheDB get cacheDB
func (d *DB) GetCacheDB(tableName string) (*CacheDB, error) {
	c, exists := d.cacheDBs[tableName]
	if !exists {
		return nil, fmt.Errorf("GetCacheDB: has not called *DB.RegisterCacheDB() to register: %s", tableName)
	}
	return c, nil
}

// CacheKey cache key info
type CacheKey struct {
	Key   string
	Value []interface{}
	isPri bool
}

var (
	// emptyCacheKey cache key is empty
	emptyCacheKey = CacheKey{}
)

// CreateCacheKeyByFields
func (c *CacheDB) CreateCacheKeyByFields(fields []string, values []interface{}) (string, error) {
	if len(fields) != len(values) {
		return "", fmt.Errorf("CreateCacheKeyByFields: fields not match values")
	}
	bs, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("CreateCacheKeyByFields: marshal values err->%v", err)
	}
	return c.module.GetKey(strings.Join(fields, "&") + gutil.BytesToString(bs)), nil
}
