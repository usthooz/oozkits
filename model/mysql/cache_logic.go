package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/henrylee2cn/goutil"
	"github.com/usthooz/gutil"
	"github.com/usthooz/oozkits/model/redis"
	"github.com/usthooz/oozlog/go"
)

// CacheKey table cache fields
type CacheKey struct {
	Key         string
	FieldValues []interface{}
	isPriKey    bool
}

var (
	emptyCacheKey = CacheKey{}
	emptyValue    = reflect.Value{}
)

// CreateCacheKeyByFields create cache key by fields
func (c *CacheDB) CreateCacheKeyByFields(fields []string, values []interface{}) (string, error) {
	// don't match
	if len(fields) != len(values) {
		return "", fmt.Errorf("CreateCacheKeyByFields(): len(fields) != len(values)")
	}
	// parse values
	bs, err := json.Marshal(values)
	if err != nil {
		return "", errors.New("CreateCacheKeyByFields(): " + err.Error())
	}
	// get key
	return c.module.GetKey(strings.Join(fields, "&") + gutil.BytesToString(bs)), nil
}

// CreateCacheKey create table cache key and fields values
func (c *CacheDB) CreateCacheKey(structPtr Cacheable, fields ...string) (CacheKey, reflect.Value, error) {
	var (
		// get type
		t = reflect.TypeOf(structPtr)
		// get name
		typeName = t.String()
	)
	// don't match
	if c.typeName != typeName {
		return emptyCacheKey, emptyValue, fmt.Errorf("CreateCacheKey(): unmatch Cacheable: want %s, have %s", c.typeName, typeName)
	}
	var (
		// get value
		value = reflect.ValueOf(structPtr).Elem()
		// make cap
		values   = make([]interface{}, 0, 2)
		cacheKey string
		// primary
		isPriKey bool
	)
	if len(fields) == 0 {
		// fields is pri
		for _, index := range c.priFieldsIndex {
			v := value.Field(index)
			values = append(values, v.Interface())
		}
		bs, err := json.Marshal(values)
		if err != nil {
			return emptyCacheKey, emptyValue, errors.New("CreateCacheKey(): " + err.Error())
		}
		// fields is primary
		isPriKey = true
		cacheKey = c.cachePriKeyPrefix + gutil.BytesToString(bs)
	} else {
		for i, field := range fields {
			fields[i] = gutil.FieldSnakeString(field)
			v := value.FieldByName(gutil.FieldsCamelString(field))
			values = append(values, v.Interface())
		}
		var (
			err error
		)
		// create cache key by fields
		cacheKey, err = c.CreateCacheKeyByFields(fields, values)
		if err != nil {
			return emptyCacheKey, emptyValue, err
		}
		if strings.HasPrefix(cacheKey, c.cachePriKeyPrefix+"[") {
			isPriKey = true
		}
	}
	return CacheKey{
		Key:         cacheKey,
		FieldValues: values,
		isPriKey:    isPriKey,
	}, value, nil
}

// createPrikey create primary key
func (c *CacheDB) createPrikey(structElemValue reflect.Value) (string, error) {
	var (
		values = make([]interface{}, 0, 2)
	)
	// get pri values
	for _, index := range c.priFieldsIndex {
		values = append(values, structElemValue.Field(index).Interface())
	}
	bs, err := json.Marshal(values)
	if err != nil {
		return "", errors.New("*CacheableDB.createPrikey(): " + err.Error())
	}
	return c.cachePriKeyPrefix + gutil.BytesToString(bs), nil
}

// CreateGetQuery get model by where fields ...
func (c *CacheDB) CreateGetQuery(whereFields ...string) string {
	if len(whereFields) == 0 {
		whereFields = c.priCols
	}
	var queryAll = "SELECT"
	for _, col := range c.cols {
		queryAll += " `" + col + "`,"
	}
	queryAll = queryAll[:len(queryAll)-1] + " FROM `" + c.tableName + "` WHERE"
	for _, col := range whereFields {
		queryAll += " `" + col + "`=? AND"
	}
	queryAll = queryAll[:len(queryAll)-4] + " LIMIT 1;"
	return queryAll
}

// cleanDestCache clean cache
func (c *CacheDB) cleanDestCache(structElemValue reflect.Value) {
	for _, i := range c.fieldsIndexMap {
		fv := structElemValue.Field(i)
		fv.Set(reflect.New(fv.Type()).Elem())
	}
}

// getFirstCache
func (c *CacheDB) getFirstCache(key string, structPtr Cacheable) (bool, error) {
	// get pri cache
	data, err := c.Cache.Get(key).Bytes()
	if err == nil {
		err = json.Unmarshal(data, structPtr)
		if err == nil {
			return true, nil
		}
		ozlog.Errorf("CacheGet(): %s", err.Error())
	} else if !redis.IsRedisNil(err) {
		return false, err
	}
	return false, nil
}

// checkSecondCache get chech second cache
func (c *CacheDB) checkSecondCache(structElemValue reflect.Value, fields []string, values []interface{}) bool {
	for i, field := range fields {
		vv := structElemValue.FieldByName(goutil.CamelString(field))
		if vv.Kind() == reflect.Ptr {
			vv = vv.Elem()
			if vv.Kind() == reflect.Invalid {
				if values[i] != nil {
					return false
				}
			} else if vv.Interface() != reflect.ValueOf(values[i]).Elem().Interface() {
				return false
			}
		} else if values[i] != vv.Interface() {
			return false
		}
	}
	return true
}

// GetCache
func (c *CacheDB) GetCache(structPtr Cacheable, fields ...string) error {
	var (
		// get cachekey anf value
		cacheKey, structElemValue, err = c.CreateCacheKey(structPtr, fields...)
	)
	if err != nil {
		return err
	}
	// use redis cache
	if c.DB.dbConfig.OpenCache {
		// get cache
		return c.DB.Get(structPtr, c.CreateGetQuery(fields...), cacheKey.FieldValues...)
	}
	var (
		key              = cacheKey.Key
		getFirstCacheKey = cacheKey.isPriKey
	)
	// read cache
	if !getFirstCacheKey {
		var (
			b []byte
		)
		b, err = c.Cache.Get(key).Bytes()
		if err == nil {
			key = gutil.BytesToString(b)
			getFirstCacheKey = true
		} else if !redis.IsRedisNil(err) {
			// get cache is nil
			return err
		}
	}
	var (
		exist bool
	)
	// get first cache
	if getFirstCacheKey {
		// clean
		c.cleanDestCache(structElemValue)
		exist, err = c.getFirstCache(key, structPtr)
		if err != nil {
			return err
		}
		if exist {
			if !cacheKey.isPriKey && !c.checkSecondCache(structElemValue, fields, cacheKey.FieldValues) {
				c.Cache.Del(cacheKey.Key)
			} else {
				return nil
			}
		}
	}
	// this lock call back
	c.Cache.LockCallback("lock_"+key, func() {
		var (
			b []byte
		)
		if !exist {
		FIRST:
			if getFirstCacheKey {
				exist, err = c.getFirstCache(key, structPtr)
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
					getFirstCacheKey = true
					goto FIRST
				} else if !redis.IsRedisNil(err) {
					return
				}
			}
		}
		// read
		err = c.DB.Get(structPtr, c.CreateGetQuery(fields...), cacheKey.FieldValues...)
		if err != nil {
			return
		}
		key, err = c.createPrikey(structElemValue)
		if err != nil {
			ozlog.Errorf("CacheGet(): createPrikey: %s", err.Error())
			err = nil
			return
		}
		// write
		data, _ := json.Marshal(structPtr)
		err = c.Cache.Set(key, data, c.cacheExpire).Err()
		if err == nil && !cacheKey.isPriKey {
			err = c.Cache.Set(cacheKey.Key, key, c.cacheExpire).Err()
		}
		if err != nil {
			ozlog.Errorf("CacheGet(): %s", err.Error())
			err = nil
		}
	})
	return err
}
