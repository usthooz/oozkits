package example

import (
	"database/sql"

	ozlog "github.com/usthooz/oozlog/go"
	"github.com/usthooz/sqlx"
)

// OozTest table struct
type OozTest struct {
	Id      int64  `key:"pri" json:"id"`
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

var (
	// OozTestSql
	OozTestSql = ""
	// oozTestDB this table db
	oozTestDB, _ = mysqlHandler.RegisterCacheDB(new(OozTest), cacheExpire, OozTestSql)
)

// TableName
func (*OozTest) TableName() string {
	return "ooztest"
}

// isZeroPrimaryKey is pri
func (_obj *OozTest) isZeroPrimaryKey() bool {
	var (
		_id int64
	)
	if _obj.Id != _id {
		return false
	}
	return true
}

// InsertOozTest
func InsertOozTest(_obj *OozTest, tx ...*sqlx.Tx) (int64, error) {
	return _obj.Id, oozTestDB.Callback(func(tx sqlx.DbAndTx) error {
		var (
			query            string
			isZeroPrimaryKey = _obj.isZeroPrimaryKey()
		)
		if isZeroPrimaryKey {
			query = "INSERT INTO `ooztest` (`name`) VALUES (:name);"
		} else {
			query = "INSERT INTO `ooztest` (`id`,`name`) VALUES (:id,:name);"
		}
		r, err := tx.NamedExec(query, _obj)
		if isZeroPrimaryKey {
			_obj.Id, err = r.LastInsertId()
		}
		return err
	}, tx...)
}

// UpsetOozTest
func UpsetOozTest(_obj *OozTest, _updateFields []string, tx ...*sqlx.Tx) (int64, error) {
	err := oozTestDB.Callback(func(tx sqlx.DbAndTx) error {
		var (
			query            string
			isZeroPrimaryKey = _obj.isZeroPrimaryKey()
		)
		if isZeroPrimaryKey {
			// insert
			query = "INSERT INTO `ooztest` (`name`) VALUES (:name)"
		} else {
			// update
			query = "INSERT INTO `ooztest` (`id`,`name`) VALUES (:id,:name)"
		}
		query += " ON DUPLICATE KEY UPDATE "
		if len(_updateFields) == 0 {
			// update all
			query += "`name`=VALUES(`name`);"
		} else {
			// upset
			for _, field := range _updateFields {
				query += "`" + field + "`=VALUES(`" + field + "`),"
			}
			if query[len(query)-1] != ',' {
				return nil
			}
			query += "`deleted`=0;"
		}
		r, err := tx.NamedExec(query, _obj)
		if isZeroPrimaryKey && err == nil {
			var (
				rowsAffected int64
			)
			rowsAffected, err = r.RowsAffected()
			if rowsAffected == 1 {
				_obj.Id, err = r.LastInsertId()
			}
		}
		return err
	}, tx...)
	if err != nil {
		return _obj.Id, err
	}
	err = oozTestDB.DeleteCache(_obj)
	if err != nil {
		ozlog.Errorf("%s", err.Error())
	}
	return _obj.Id, nil
}

// UpdateOozTestByPrimary
func UpdateOozTestByPrimary(_obj *OozTest, _updateFields []string, tx ...*sqlx.Tx) error {
	err := oozTestDB.Callback(func(tx sqlx.DbAndTx) error {
		query := "UPDATE `ooztest` SET "
		if len(_updateFields) == 0 {
			// update all
			query += "`name`:=name WHERE `id`=:id LIMIT 1;"
		} else {
			// upset
			for i, field := range _updateFields {
				if i == len(_updateFields)-1 {
					query += "`" + field + "`=:" + field
				} else {
					query += "`" + field + "`=:" + field + ","
				}
			}
			if query[len(query)-1] != ',' {
				return nil
			}
			query += " WHERE `id`=:id LIMIT 1;"
		}
		_, err := tx.NamedExec(query, _obj)
		return err
	}, tx...)
	if err != nil {
		return err
	}
	err = oozTestDB.DeleteCache(_obj)
	if err != nil {
		ozlog.Errorf("%s", err.Error())
	}
	return nil
}

// GetOozTestByPrimary
func GetOozTestByPrimary(_id int64) (*OozTest, bool, error) {
	var _obj = &OozTest{
		Id: _id,
	}
	err := oozTestDB.GetCache(_obj)
	switch err {
	case nil:
		return _obj, true, nil
	case sql.ErrNoRows:
		return nil, false, nil
	default:
		return nil, false, err
	}
}

// GetOozTestFirst
func GetOozTestFirst(whereConds string, args ...interface{}) (*OozTest, bool, error) {
	var (
		obj = new(OozTest)
	)
	err := oozTestDB.Get(obj, "SELECT `id`,`name`,`deleted` FROM `ooztest` WHERE "+whereConds+" LIMIT 1;", args...)
	switch err {
	case nil:
		return obj, true, nil
	case sql.ErrNoRows:
		return nil, false, nil
	default:
		return nil, false, err
	}
}

// GetOozTestByWhere
func GetOozTestByWhere(whereConds string, args ...interface{}) ([]*OozTest, error) {
	var (
		_objs = new([]*OozTest)
	)
	err := oozTestDB.Select(_objs, "SELECT `id`,`name`,`deleted` FROM `ooztest` WHERE "+whereConds, args...)
	return *_objs, err
}

// CountOozTestByWhere
func CountOozTestByWhere(whereConds string, args ...interface{}) (int64, error) {
	var (
		count int64
	)
	err := oozTestDB.Get(&count, "SELECT count(1) FROM `ooztest` WHERE "+whereConds, args...)
	return count, err
}
