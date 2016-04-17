package shred

import (
	"os"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
)

func CassandraSession(t *testing.T) *gocql.Session {
	host := os.Getenv("CASSANDRA_HOST")

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = "shred_test"
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		t.Skip("unable to connect to Cassandra")
	}

	return session
}

func TestCassandraIterator(t *testing.T) {
	session := CassandraSession(t)
	defer session.Close()

	users := NewCassandraIterator(session, "SELECT user_id, first_name, last_name FROM users")
	orders := NewCassandraIterator(session, "SELECT user_id, total_price FROM orders")

	expected := []Record{
		{"user_id": 2, "first_name": "Jane", "last_name": "Smith", "total_price": 11},
		{"user_id": 1, "first_name": "John", "last_name": "Smith", "total_price": 25},
		{"user_id": 1, "first_name": "John", "last_name": "Smith", "total_price": 55},
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
