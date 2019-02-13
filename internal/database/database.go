package database

import (
	"exporter/internal/config"
	"fmt"
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
	stmt          *sqlx.Stmt
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
		panic(err)
	}

	return &DB{db}
}

// ExportCSV exports csv
func (DB *DB) ExportCSV(conf config.Configuration, threads int) {
	queriesToExecute := make(chan queryParams)
	noMoreQueries := make(chan bool)
	var dones []chan bool

	for i := 0; i < threads; i++ {
		done := make(chan bool)
		dones = append(dones, done)

		go func() {
			for true {
				select {
				case q := <-queriesToExecute:
					rows, err := q.stmt.Queryx()

					if err != nil {
						panic(err)
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
									panic(err)
								}

								fileName := fmt.Sprintf("%s/%s/%03d.csv", q.outputDirPath, q.tableName, fileI)
								nextFile <- fileInfo{fileName: fileName, columns: columns}
								fileI++
							}

							row, err := rows.SliceScan()

							if err != nil {
								panic(err)
							}

							rowsToDump <- row
							rowI++
						}
					}
					noMoreRows <- true
					<-doneDumping
					rows.Close()

				case <-noMoreQueries:
					break
				}
			}
			done <- true
		}()
	}

	for _, tbl := range conf.Tables {
		psQuery, err := DB.Database.Preparex(tbl.Query)

		if err != nil {
			panic(err)
		}

		queriesToExecute <- queryParams{stmt: psQuery, maxLines: tbl.MaxLines, tableName: tbl.Name, outputDirPath: conf.OutputDir}
	}
	noMoreQueries <- true

	for _, done := range dones {
		<-done
	}
}

func writeCSV(nextFile chan fileInfo, rowsToDump chan []interface{}, noMoreRows chan bool, doneDumping chan bool) {
	var currentFile fileInfo
	var file *os.File
	for true {
		select {
		case r := <-rowsToDump:

			if file == nil {
				if currentFile.fileName == "" {
					panic("Received data before the file name.")
				} else {
					if _, err := os.Stat(currentFile.fileName); err != nil {
						err := os.MkdirAll(path.Dir(currentFile.fileName), 0777)

						if err != nil {
							panic(err)
						}

					}

					file, err := os.Create(currentFile.fileName)

					if err != nil {
						panic(err)
					}

					file.Chmod(0777)

					for idx, col := range currentFile.columns {
						if idx == len(currentFile.columns)-1 {
							_, err := file.WriteString(col)

							if err != nil {
								panic(err)
							}

						} else {
							_, err := file.WriteString(col + ",")

							if err != nil {
								panic(err)
							}

						}
					}
					_, err = file.WriteString("\n")

					if err != nil {
						panic(err)
					}

				}
			}

			for idx, el := range r {
				switch el := el.(type) {
				case string:
					if idx == len(r)-1 {
						_, err := file.WriteString(el)

						if err != nil {
							panic(err)
						}

					} else {
						_, err := file.WriteString(el + ",")

						if err != nil {
							panic(err)
						}

					}
				case int, int8, int32, int64:
					if idx == len(r)-1 {
						_, err := file.WriteString(strconv.FormatInt(el.(int64), 10))

						if err != nil {
							panic(err)
						}

					} else {
						_, err := file.WriteString(strconv.FormatInt(el.(int64), 10) + ",")

						if err != nil {
							panic(err)
						}

					}
				case time.Time:
					if idx == len(r)-1 {
						_, err := file.WriteString(strconv.FormatInt(el.Unix(), 10))

						if err != nil {
							panic(err)
						}

					} else {
						_, err := file.WriteString(strconv.FormatInt(el.Unix(), 10) + ",")

						if err != nil {
							panic(err)
						}

					}
				default:
					panic("Unknown column data type.")
				}

			}
			_, err := file.WriteString("\n")

			if err != nil {
				panic(err)
			}

		case f := <-nextFile:

			if file != nil {
				err := file.Sync()

				if err != nil {
					panic(err)
				}

				err = file.Close()

				if err != nil {
					panic(err)
				}

				file = nil
			}

			currentFile = f

		case <-noMoreRows:

			if file != nil {
				err := file.Sync()

				if err != nil {
					panic(err)
				}

				err = file.Close()

				if err != nil {
					panic(err)
				}

				file = nil
			}

			break
		}
	}
	doneDumping <- true
}
