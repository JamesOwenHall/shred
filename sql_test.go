package shred

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func MysqlConnection(t *testing.T) *sql.DB {
	host, user, pass := os.Getenv("MYSQL_HOST"), os.Getenv("MYSQL_USER"), os.Getenv("MYSQL_PASS")

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/shred_test", user, pass, host))
	if err != nil {
		t.Skip("unable to connect to MySQL")
	} else if err := db.Ping(); err != nil {
		t.Skip("unable to connect to MySQL")
	}

	return db
}

func TestSqlIterator(t *testing.T) {
	db := MysqlConnection(t)
	defer db.Close()

	users := NewSqlIterator(db, "SELECT user_id, first_name, last_name FROM users")
	orders := NewSqlIterator(db, "SELECT user_id, total_price FROM orders")

	expected := []Record{
		{"user_id": "2", "first_name": "Jane", "last_name": "Smith", "total_price": "11"},
		{"user_id": "1", "first_name": "John", "last_name": "Smith", "total_price": "25"},
		{"user_id": "1", "first_name": "John", "last_name": "Smith", "total_price": "55"},
	}

	actual, err := NewDataset(users).
		InnerJoin("user_id", "user_id", orders).
		SortInt("total_price").
		Collect()

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("\nexpected: %v\n  actual: %v", expected, actual)
	}
}
