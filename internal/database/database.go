package database

import (
	"exporter/internal/config"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	Database *sqlx.DB
}

type queryParams struct {
	stmt          string
	maxLines      int
	tableName     string
	outputDirPath string
}

type fileInfo struct {
	fileName string
	columns  []string
}

func Connect(conf config.Configuration) *DB {
	db, err := sqlx.Connect("postgres", conf.Connector)
	if err != nil {
		log.Fatal(err)
	}

	return &DB{db}
}

// ExportCSV exports csv
func (DB *DB) ExportCSV(conf config.Configuration, threads int) {
	queriesToExecute := make(chan queryParams)
	noMoreQueries := make(chan bool)

	for i := 0; i < threads; i++ {
		go DB.executeQueries(queriesToExecute, noMoreQueries)
	}

	for _, tbl := range conf.Tables {
		queriesToExecute <- queryParams{
			stmt:          tbl.Query,
			maxLines:      tbl.MaxLines,
			tableName:     tbl.Name,
			outputDirPath: conf.OutputDir}
	}

	for i := 0; i < threads; i++ {
		noMoreQueries <- true
	}
}

func writeCSV(nextFile chan fileInfo, rowsToDump chan []interface{}, noMoreRows chan bool, doneDumping chan bool) {
	var currentFile fileInfo
	var file *os.File

loop:
	for true {
		select {
		case r := <-rowsToDump:

			if file == nil {
				if currentFile.fileName == "" {
					log.Fatal("Received data before the file name.")
				}

				err := os.MkdirAll(path.Dir(currentFile.fileName), 0777)
				if err != nil {
					log.Fatal(err)
				}

				file, err = os.Create(currentFile.fileName)
				if err != nil {
					log.Fatal(err)
				}

				err = file.Chmod(0777)
				if err != nil {
					log.Fatal(err)
				}

				for idx, col := range currentFile.columns {
					if idx > 0 {
						_, err = file.WriteString(",")
						if err != nil {
							log.Fatal(err)
						}
					}

					_, err = file.WriteString(col)
					if err != nil {
						log.Fatal(err)
					}
				}

				_, err = file.WriteString("\n")
				if err != nil {
					log.Fatal(err)
				}
			}

			for idx, el := range r {
				if idx > 0 {
					_, err := file.WriteString(",")
					if err != nil {
						log.Fatal(err)
					}
				}

				switch el := el.(type) {
				case string:

					_, err := file.WriteString(el)
					if err != nil {
						log.Fatal(err)
					}

				case int, int8, int32, int64:

					_, err := file.WriteString(strconv.FormatInt(el.(int64), 10))
					if err != nil {
						log.Fatal(err)
					}

				case time.Time:

					_, err := file.WriteString(strconv.FormatInt(el.Unix(), 10))
					if err != nil {
						log.Fatal(err)
					}

				case bool:

					_, err := file.WriteString(strconv.FormatBool(el))
					if err != nil {
						log.Fatal(err)
					}

				case []uint8:

					b := make([]byte, len(el))
					for i, v := range el {
						b[i] = byte(v)
					}

					_, err := file.WriteString(string(b))
					if err != nil {
						log.Fatal(err)
					}

				case nil:
					_, err := file.WriteString("NULL")
					if err != nil {
						log.Fatal(err)
					}

				default:
					log.Fatal("Unknown column data type: ", el)

				}
			}

			_, err := file.WriteString("\n")
			if err != nil {
				log.Fatal(err)
			}

		case f := <-nextFile:

			if file != nil {
				err := file.Sync()
				if err != nil {
					log.Fatal(err)
				}

				err = file.Close()
				if err != nil {
					log.Fatal(err)
				}

				file = nil
			}

			currentFile = f

		case <-noMoreRows:

			if file != nil {
				err := file.Sync()
				if err != nil {
					log.Fatal(err)
				}

				err = file.Close()
				if err != nil {
					log.Fatal(err)
				}

				file = nil
			}

			break loop

		}
	}

	doneDumping <- true
}

func (DB *DB) executeQueries(queriesToExecute chan queryParams, noMoreQueries chan bool) {
loop:
	for true {
		select {
		case q := <-queriesToExecute:

			rows, err := DB.Database.Queryx(q.stmt)
			if err != nil {
				log.Fatal(err)
			}

			nextFile := make(chan fileInfo)
			rowsToDump := make(chan []interface{})
			noMoreRows := make(chan bool)
			doneDumping := make(chan bool)

			go writeCSV(nextFile, rowsToDump, noMoreRows, doneDumping)

			fileI := 0
			for results := true; results; results = rows.NextResultSet() {
				rowI := 0
				for rows.Next() {
					if rowI >= q.maxLines {
						rowI = 0
					}

					if rowI == 0 {
						columns, err := rows.Columns()
						if err != nil {
							log.Fatal(err)
						}

						fileName := fmt.Sprintf("%s/%s/%03d.csv", q.outputDirPath, q.tableName, fileI)
						nextFile <- fileInfo{fileName: fileName, columns: columns}
						fileI++
					}

					row, err := rows.SliceScan()
					if err != nil {
						log.Fatal(err)
					}

					rowsToDump <- row
					rowI++
				}
			}

			noMoreRows <- true
			<-doneDumping
			rows.Close()

		case <-noMoreQueries:

			break loop

		}
	}
}
