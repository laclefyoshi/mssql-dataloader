package cmd

import (
	"database/sql"
	"fmt"
	"net/url"
	"log"
)

func openSQL(server, database, user, password string) *sql.DB {
	query := url.Values{}
	query.Add("database", database)
	query.Add("connection timeout", "0")
	connStr := &url.URL{
		Scheme: "sqlserver",
		Host: fmt.Sprintf("%s:%d", server, 1433),
		User: url.UserPassword(user, password),
		RawQuery: query.Encode(),
	}
	db, err := sql.Open("sqlserver", connStr.String())
	if err != nil {
		log.Fatal(err)
	}
	return db
}
