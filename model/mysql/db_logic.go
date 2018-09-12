package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/usthooz/gutil"
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
func (c *CacheDB) CreateCacheKey(structStr Cacheable, fields ...string) (CacheKey, reflect.Value, error) {
	var (
		// get type
		t = reflect.TypeOf(structStr)
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
