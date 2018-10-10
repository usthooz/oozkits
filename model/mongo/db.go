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
	ozlog "github.com/usthooz/oozlog/go"
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

// getSession
func (c *CacheDB) getSession() (*mgo.Session, error) {
	if c.DB.Session.Ping() != nil {
		session, err := mgo.DialWithInfo(c.DB.dbConfig.Source())
		if err != nil {
			return nil, err
		}
		c.DB.Session = session
	}
	return c.DB.Session.Clone(), nil
}

// RegisterCacheDB register cache db
func (d *DB) RegisterCacheDB(structPtr Cacheable, cacheExpire time.Duration) (*CacheDB, error) {
	// get table name
	tableName := structPtr.TableName()
	if _, exists := d.cacheDBs[tableName]; exists {
		return nil, fmt.Errorf("Already register cache and table name->%s", tableName)
	}
	// if open cache and rds is nil
	if !d.dbConfig.CloseCache && d.Cache == nil {
		return nil, ErrCacheIsNil
	}
	// new rds module
	module := redis.NewModule(d.dbConfig.Database + ":" + tableName)
	t := reflect.TypeOf(structPtr)
	c := &CacheDB{
		DB:                d,
		tableName:         tableName,
		cachePriKeyPrefix: module.GetKey("_id"),
		cacheExpire:       cacheExpire,
		typeName:          t.String(),
		module:            module,
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

// GetCache
func (c *CacheDB) GetCache(destStructPtr Cacheable, fields ...string) error {
	var (
		cacheKey, err = c.CreateCacheKey(destStructPtr, fields...)
	)
	if err != nil {
		return err
	}
	if c.DB.dbConfig.CloseCache {
		// cache is closed
		return c.WitchCollection(func(collect *mgo.Collection) error {
			return collect.Find(c.CreateGetQuery(cacheKey.Values, fields...)).One(destStructPtr)
		})
	}
	var (
		key                 = cacheKey.Key
		gettedFirstCacheKey = cacheKey.isPri
	)

	// read secondary cache
	if !gettedFirstCacheKey {
		var b []byte
		b, err = c.Cache.Get(key).Bytes()
		if err == nil {
			key = goutil.BytesToString(b)
			gettedFirstCacheKey = true
		} else if !redis.IsRedisNil(err) {
			return err
		}
	}

	var (
		exist bool
	)

	// get first cache
	if gettedFirstCacheKey {
		exist, err = c.getFirstCache(key, destStructPtr)
		if err != nil {
			return err
		}
		if exist {
			// check
			if !cacheKey.isPri && !c.checkSecondCache(destStructPtr, fields, cacheKey.Values) {
				c.Cache.Del(cacheKey.Key)
			} else {
				return nil
			}
		}
	}
	c.Cache.LockCallback("lock_"+key, func() {
		var b []byte
		if !exist {
		FIRST:
			if gettedFirstCacheKey {
				exist, err = c.getFirstCache(key, destStructPtr)
				if exist {
					err = nil
					return
				}
				if err != nil {
					return
				}
			} else {
				b, err = c.Cache.Get(key).Bytes()
				if err == nil {
					key = gutil.BytesToString(b)
					gettedFirstCacheKey = true
					goto FIRST
				} else if !redis.IsRedisNil(err) {
					return
				}
			}
		}
		err = c.WitchCollection(func(collect *mgo.Collection) error {
			return collect.Find(c.CreateGetQuery(cacheKey.Values, fields...)).One(destStructPtr)
		})
		if err != nil {
			return
		}

		key, err = c.createPrikey(destStructPtr)
		if err != nil {
			ozlog.Errorf("GetCache(): createPrikey: %s", err.Error())
			err = nil
			return
		}

		// write cache
		data, _ := json.Marshal(destStructPtr)
		err = c.Cache.Set(key, data, c.cacheExpire).Err()
		if err == nil && !cacheKey.isPri {
			err = c.Cache.Set(cacheKey.Key, key, c.cacheExpire).Err()
		}
		if err != nil {
			ozlog.Errorf("GetCache(): %s", err.Error())
			err = nil
		}
	})
	return err
}

// checkSecondCache
func (c *CacheDB) checkSecondCache(destStructPtr Cacheable, fields []string, values []interface{}) bool {
	v := reflect.ValueOf(destStructPtr).Elem()
	for i, field := range fields {
		vv := v.FieldByName(goutil.CamelString(field))
		if vv.Kind() == reflect.Ptr {
			vv = vv.Elem()
		}
		if values[i] != vv.Interface() {
			return false
		}
	}
	return true
}

// getFirstCache
func (c *CacheDB) getFirstCache(key string, destStructPtr Cacheable) (bool, error) {
	data, err := c.Cache.Get(key).Bytes()
	if err == nil {
		err = json.Unmarshal(data, destStructPtr)
		if err == nil {
			return true, nil
		}
		ozlog.Errorf("CacheGet(): %s", err.Error())

	} else if !redis.IsRedisNil(err) {
		return false, err
	}
	return false, nil
}

// WitchCollection
func (c *CacheDB) WitchCollection(s func(*mgo.Collection) error) error {
	session, err := c.getSession()
	if err != nil {
		return fmt.Errorf("Mongodb connection error:%s", err)
	}
	defer func() {
		session.Close()
		if err := recover(); err != nil {
			ozlog.Errorf("Mongodb close session err:%s", err)
		}
	}()
	collection := c.DB.DB(c.DB.dbConfig.Database).C(c.tableName)
	return s(collection)
}

// createPrikey
func (c *CacheDB) createPrikey(structPtr Cacheable) (string, error) {
	var v = reflect.ValueOf(structPtr).Elem()
	objectIdHex := bson.ObjectId(v.FieldByName("Id").String())
	values := []interface{}{
		objectIdHex.Hex(),
	}
	bs, err := json.Marshal(values)
	if err != nil {
		return "", errors.New("*CacheableDB.createPrikey(): " + err.Error())
	}
	return c.cachePriKeyPrefix + goutil.BytesToString(bs), nil
}

// CreateGetQuery
func (c *CacheDB) CreateGetQuery(values []interface{}, whereConds ...string) bson.M {
	m := bson.M{}
	if len(whereConds) == 0 {
		return m
	}
	for index, col := range whereConds {
		m[col] = values[index]
	}
	return m
}
