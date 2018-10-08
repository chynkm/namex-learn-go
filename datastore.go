package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type datastore struct {
	*sql.DB
}

type DomainName struct {
	Id     int64
	Domain string
}

func open() (*datastore, error) {
	db, err := sql.Open("mysql", "root:@tcp(192.168.1.2)/namex?parseTime=true")

	if err != nil {
		log.Panic(err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.Panic(err)
		return nil, err
	}
	return &datastore{db}, nil
}
