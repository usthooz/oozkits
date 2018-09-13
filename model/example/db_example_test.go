package example

import (
	"testing"
	"time"

	"github.com/usthooz/oozkits/model/mysql"
	"github.com/usthooz/oozkits/model/redis"
	ozlog "github.com/usthooz/oozlog/go"
)

func init() {
	mysqlConfig := &mysql.Config{
		Username: "root",
		Password: "0707",
		Database: "ooz",
		Host:     "127.0.0.1",
		Port:     3306,
	}
	redisConfig := &redis.Config{
		ForSingle: redis.SingleConfig{
			Addr: "127.0.0.1:6379",
		},
		DeployType: "single",
	}
	// init
	if err := Init(mysqlConfig, redisConfig, time.Hour); err != nil {
		ozlog.Fatalf("Init err->%v", err)
	}
}

func TestDbMethod(t *testing.T) {
	// insert
	id, err := InsertOozTest(&OozTest{
		Name:    "insert",
		Deleted: false,
	})
	if err != nil {
		t.Fatalf("Insert Id: %d", id)
	}
	t.Logf("insert success->%d", id)
	time.Sleep(time.Second * 5)
	// get by pri
	ooz, exists, err := GetOozTestByPrimary(id)
	if err != nil {
		t.Fatalf("Get By pri err->%v", err)
	}
	if !exists {
		t.Fatal("This model no exists.")
	}
	t.Logf("get by pri success->%s", ooz.Name)
	time.Sleep(time.Second * 5)
	ooz.Name = "upset"
	// upset test
	_, err = UpsetOozTest(ooz, []string{"name"})
	if err != nil {
		t.Logf("upset err->%v", err)
	}
	t.Log("upset success.")
	time.Sleep(time.Second * 5)
	// get first
	ooztest, exists, err := GetOozTestFirst("`id`=?", id)
	if err != nil {
		t.Fatalf("get first err->%v", err)
	}
	if !exists {
		t.Fatal("this model non exists.")
	}
	t.Logf("get first success->%d", ooztest.Id)
	time.Sleep(time.Second * 5)
	ooztest.Name = "update"
	// update by pri
	err = UpdateOozTestByPrimary(ooztest, []string{"name"})
	if err != nil {
		t.Fatalf("update err->%v", err)
	}
	t.Log("update seuccess.")
	time.Sleep(time.Second * 5)
	// get by where
	oozs, err := GetOozTestByWhere("`deleted`=?", false)
	if err != nil {
		t.Fatalf("get test by where err->%v", err)
	}
	t.Logf("result len: %d", len(oozs))
	time.Sleep(time.Second * 5)
	// get count
	total, err := CountOozTestByWhere("`deleted`=?", false)
	if err != nil {
		t.Fatalf("get count err->%v", err)
	}
	t.Logf("Count: %d", total)
}
