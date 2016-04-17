#!/bin/sh

if [[ -n "$CASSANDRA_HOST" ]]; then
    echo "Setting up Cassandra..."
    cqlsh -f script/setup_cassandra.cql "$CASSANDRA_HOST"
fi

if [[ -n "$MYSQL_HOST" ]]; then
    echo "Setting up MySQL..."
    mysql --host="$MYSQL_HOST" --user="$MYSQL_USER" --password="$MYSQL_PASS" < script/setup_mysql.sql
fi
