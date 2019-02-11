package database

import (
	"exporter/internal/config"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	Database *sqlx.DB
}

func Connect(conf config.Configuration) *DB {
	db, err := sqlx.Connect("postgres", conf.Connector)
	if err != nil {
		log.Println(err)
		log.Println("Can't connect to Postgres.")
		os.Exit(1)
	}

	return &DB{db}
}

func (DB *DB) Export(conf config.Configuration) {

}
