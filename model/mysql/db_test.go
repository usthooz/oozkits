package mysql

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/usthooz/oozkits/model/redis"
)

type ooztestTable struct {
	Id      int64
	Name    string
	Deleted bool `json:"deleted"`
}

func (t *ooztestTable) TableName() string {
	return "ooztest"
}

func TestMysqlAndCache(t *testing.T) {
	// db config
	dbconfig := &Config{
		Database: "ooz",
		Username: "root",
		Password: "0707",
		Host:     "127.0.0.1",
		Port:     3306,
	}
	// rds config
	single := redis.SingleConfig{
		Addr: "127.0.0.1:6379",
	}
	rdsConfig := &redis.Config{
		DeployType: "single",
		ForSingle:  single,
	}
	db, err := Connect(dbconfig, rdsConfig)
	if err != nil {
		t.Fatal(err)
	}
	// create table
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS `ooztest` (`id` INT(10) AUTO_INCREMENT, `name` VARCHAR(20), `deleted` TINYINT(2),  PRIMARY KEY(`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试表'")
	if err != nil {
		t.Fatal(err)
	}
	// register
	c, err := db.RegisterCacheDB(new(ooztestTable), time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	obj := &ooztestTable{
		Id:      1,
		Name:    "usth",
		Deleted: false,
	}
	// insert
	_, err = c.NamedExec("INSERT INTO ooztest (id,name,deleted)VALUES(:id,:name,:deleted) ON DUPLICATE KEY UPDATE id=:id", obj)
	if err != nil {
		t.Fatal(err)
	}

	// get cache
	dest := &ooztestTable{Id: 1}
	err = c.GetCache(dest)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cache:%#v", dest)

	// create the cache
	cacheKey, _, err := c.CreateCacheKey(dest)
	t.Logf("cacheKey:%#v", cacheKey)
	key := cacheKey.Key
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("key: %s", key)
	// get cache
	b, err := c.Cache.Get(key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var (
		value = new(ooztestTable)
	)
	err = json.Unmarshal(b, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cache info: %+v", value)
}
