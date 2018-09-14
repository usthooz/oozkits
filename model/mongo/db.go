package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/henrylee2cn/goutil"
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
	t := reflect.TypeOf(structPtr)
	c := &Cacheable{
		DB:          d,
		tableName:   tableName,
		cacheExpire: module.GetKey("_id"),
		cacheExpire: cacheExpire,
		typeName:    t.String(),
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
	Key    string
	Values []interface{}
	isPri  bool
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

// CreateCacheKey
func (c *CacheDB) CreateCacheKey(structPtr Cacheable, fields ...string) (CacheKey, error) {
	var (
		t        = reflect.TypeOf(structPtr)
		typeName = t.String()
	)
	// type not match
	if c.typeName != typeName {
		return emptyCacheKey, fmt.Errorf("CreateCacheKey(): unmatch Cache: want %s, have %s", c.typeName, typeName)
	}
	var (
		v        = reflect.ValueOf(structPtr).Elem()
		values   = make([]interface{}, 0, 2)
		cacheKey string
	)
	// fields is required
	if len(fields) == 0 {
		return emptyCacheKey, errors.New("CreateCacheKey(): fields is required.")
	} else {
		for i, field := range fields {
			value := v.FieldByName(goutil.CamelString(field))
			if value.Kind() == reflect.Ptr {
				value = value.Elem()
			}
			values = append(values, value.Interface())
			fields[i] = gutil.FieldSnakeString(field)
		}
		var (
			err error
		)
		cacheKey, err = c.CreateCacheKeyByFields(fields, values)
		if err != nil {
			return emptyCacheKey, err
		}
	}
	return CacheKey{
		Key:    cacheKey,
		Values: values,
	}, nil
}
