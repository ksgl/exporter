package database

import (
	"exporter/internal/config"
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/xwb1989/sqlparser"
)

type DB struct {
	Database *sqlx.DB
}

func Connect(conf config.Configuration) *DB {
	db, err := sqlx.Connect("postgres", conf.Connector)
	if err != nil {
		log.Println("Can't connect to Postgres.")
		os.Exit(1)
	}

	return &DB{db}
}

// ExportCSV exports csv
func (DB *DB) ExportCSV(conf config.Configuration) {
	outputDirPath := conf.OutputDir
	if _, err := os.Stat(outputDirPath); err != nil {
		os.Mkdir(outputDirPath, 0777)
	}

	for i, tbl := range conf.Tables {
		columns, table := parseSelectQuery(tbl.Query)

		// create directory for each table
		tableOutputDirPath := outputDirPath + "/" + table
		if _, err := os.Stat(tableOutputDirPath); err != nil {
			os.Mkdir(tableOutputDirPath, 0777)
		}

		// execute queries
		DB.execQuery(tbl.Query, len(columns))

		// create file and write to it
		fileName := fmt.Sprintf("%s/%d.csv", tableOutputDirPath, i)
		file, _ := os.Create(fileName)
		file.Chmod(0777)

		file.Close()
	}
}

func parseSelectQuery(query string) (columns []string, table string) {
	stmt, _ := sqlparser.Parse(query)

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		for _, col := range stmt.SelectExprs {
			columns = append(columns, sqlparser.String(col))
		}
		table = sqlparser.String(stmt.From)
	}

	return columns, table
}

func (DB *DB) execQuery(query string, count int) (rowsInterface []interface{}) {
	rows, err := DB.Database.Queryx(query)

	scanned := make([]interface{}, count)
	for rows.Next() {
		err = rows.Scan(scanned...)
	}

	log.Println(err)

	for i := 0; i < len(scanned); i++ {
		fmt.Println(scanned[i])
	}

	return scanned
}
