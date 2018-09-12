package mysql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/usthooz/gutil"
	"github.com/usthooz/oozkits/model/redis"
	"github.com/usthooz/sqlx"
	"github.com/usthooz/sqlx/reflectx"
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
	cacheExpire       time.Duration
	typeName          string
	priFieldsIndex    []int          // primary column index in struct
	fieldsIndexMap    map[string]int // key:colName, value:field index in struct
	module            *redis.Module
}

// ErrCacheIsNil db cache(redis) is nil
var ErrCacheIsNil = errors.New("*DB.Cache (redis) is nil")

// Cacheable
type Cacheable interface {
	TableName() string
}

// Column db column
type Column struct {
	ColumnName string `json:"COLUMN_NAME" db:"COLUMN_NAME"`
	ColumnKey  string `json:"COLUMN_KEY" db:"COLUMN_KEY"`
}

// RegisterCacheDB register a cache table.
func (d *DB) RegisterCacheDB(ormStruct Cacheable, expire time.Duration) (*CacheDB, error) {
	// get db table name
	tableName := ormStruct.TableName()
	// check this table cache is exists?
	if _, ok := d.cacheDBs[tableName]; ok {
		return nil, fmt.Errorf("This table cache already registered, table->%s.", tableName)
	}
	// this is use cache?
	if !d.dbConfig.OpenCache && d.Cache == nil {
		return nil, ErrCacheIsNil
	}
	var (
		cols []*Column
	)
	// get table attribute
	err := d.DB.Select(&cols, `SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.columns WHERE table_schema = ? AND table_name = ?;`, d.dbConfig.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("RegCacheableDB(): %s", err.Error())
	}
	var (
		priCols, generalCols []string
	)
	for _, col := range cols {
		// table pri
		if col.ColumnKey == "PRI" {
			priCols = append(priCols, col.ColumnName)
		}
		generalCols = append(generalCols, col.ColumnName)
	}
	if len(priCols) == 0 {
		return nil, fmt.Errorf("table has no primary key, table->%s", tableName)
	}
	sort.Strings(priCols)
	t := reflect.TypeOf(ormStruct)
	var (
		typeName = t.String()
	)
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Orm Struct must be *struct type: %s", typeName)
	}

	structMap := d.Mapper.TypeMap(t)
	var (
		fieldsIndexMap = make(map[string]int, len(structMap.Index))
	)
	for i, index := range structMap.Index {
		for _, col := range generalCols {
			if col == index.Name {
				fieldsIndexMap[col] = i
				break
			}
		}
	}
	var (
		priFieldsIndex = make([]int, len(priCols))
	)
	for i, col := range priCols {
		priFieldsIndex[i] = fieldsIndexMap[col]
	}
	module := redis.NewModule(d.dbConfig.Database + ":" + tableName)
	c := &CacheDB{
		DB:                d,
		tableName:         tableName,
		cachePriKeyPrefix: module.GetKey(strings.Join(priCols, "&")),
		cols:              generalCols,
		priCols:           priCols,
		cacheExpire:       expire,
		typeName:          typeName,
		priFieldsIndex:    priFieldsIndex,
		fieldsIndexMap:    fieldsIndexMap,
		module:            module,
	}
	d.cacheDBs[tableName] = c
	return c, nil
}

// GetCacheDB
func (d *DB) GetCacheDB(tableName string) (*CacheDB, error) {
	// use table name get cache db
	c, ok := d.cacheDBs[tableName]
	if !ok {
		return nil, fmt.Errorf("has not called *DB.RegCacheableDB() to register: %s", tableName)
	}
	return c, nil
}
