package database

import (
	"exporter/internal/config"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

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
	// создаем папку results, если её не существует
	outputDirPath := conf.OutputDir
	if _, err := os.Stat(outputDirPath); err != nil {
		os.Mkdir(outputDirPath, 0777)
	}

	for _, tbl := range conf.Tables {
		// получаем из запроса колонки и имя таблицы (для хедера и названия файлов)
		columns, colSize, table := parseSelectQuery(tbl.Query)

		// для таблицы создаем папку, если её не существует
		tableOutputDirPath := outputDirPath + "/" + table
		if _, err := os.Stat(tableOutputDirPath); err != nil {
			os.Mkdir(tableOutputDirPath, 0777)
		}

		// получаем данные селекта и размер данных
		data, dataSize, rowSize := DB.execSelectQuery(tbl.Query)

		// определяем количество файлов
		filesCount := 1
		if tbl.MaxLines < dataSize {
			filesCount = int(math.Ceil(float64(dataSize) / float64(tbl.MaxLines)))
		}

		// создаём файлы и записываем в них данные
		for i := 0; i < filesCount; i++ {
			fileName := fmt.Sprintf("%s/%03d.csv", tableOutputDirPath, i+1)
			file, _ := os.Create(fileName)
			file.Chmod(0777)

			for idx, col := range columns {
				if idx == colSize-1 {
					file.WriteString(col)
				} else {
					file.WriteString(col + ",")
				}
			}
			file.WriteString("\n")

			for _, row := range data {
				for idx, el := range row {
					switch el := el.(type) {
					case string:
						if idx == rowSize-1 {
							file.WriteString(el)
						} else {
							file.WriteString(el + ",")
						}
					case int, int8, int32, int64:
						if idx == rowSize-1 {
							file.WriteString(strconv.FormatInt(el.(int64), 10))
						} else {
							file.WriteString(strconv.FormatInt(el.(int64), 10) + ",")
						}
					case time.Time:
						if idx == rowSize-1 {
							file.WriteString(strconv.FormatInt(el.Unix(), 10))
						} else {
							file.WriteString(strconv.FormatInt(el.Unix(), 10) + ",")
						}
					default:
						log.Println("Unknown type.")
					}
				}
				file.WriteString("\n")
			}

			file.Close()
		}
	}
}

// функция возвращает запрашиваемые селектом колонки и название таблицы
func parseSelectQuery(query string) (columns []string, colSize int, table string) {
	stmt, _ := sqlparser.Parse(query)

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		for _, col := range stmt.SelectExprs {
			columns = append(columns, sqlparser.String(col))
		}
		table = sqlparser.String(stmt.From)
	}

	return columns, len(columns), table
}

// функция выполняет селект (не зависит от количества запрашиваемых колонок, универсальна)
func (DB *DB) execSelectQuery(query string) (data [][]interface{}, dataSize, rowSize int) {
	rows, _ := DB.Database.Queryx(query)

	for rows.Next() {
		row, _ := rows.SliceScan()
		data = append(data, row)
	}
	rows.Close()

	return data, len(data), len(data[0])
}
