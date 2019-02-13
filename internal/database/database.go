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
		log.Println("Can't connect to Postgres.")
		os.Exit(1)
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
					log.Println("qwery obtained")

					rows, _ := q.stmt.Queryx()

					nextFile := make(chan fileInfo)
					rowsToDump := make(chan []interface{})
					noMoreRows := make(chan bool)
					doneDumping := make(chan bool)

					go func() {
						fileName := ""
						var columns []string
						var file *os.File
						for true {
							select {
							case r := <-rowsToDump:

								if file == nil {
									if fileName == "" {
										log.Fatal("File doesn't exist.")
									} else {
										if _, err := os.Stat(fileName); err != nil {
											os.MkdirAll(path.Dir(fileName), 0777)
										}

										file, _ = os.Create(fileName)
										file.Chmod(0777)

										for idx, col := range columns {
											if idx == len(columns)-1 {
												file.WriteString(col)
											} else {
												file.WriteString(col + ",")
											}
										}
										file.WriteString("\n")
									}
								}

								for idx, el := range r {
									switch el := el.(type) {
									case string:
										if idx == len(r)-1 {
											file.WriteString(el)
										} else {
											file.WriteString(el + ",")
										}
									case int, int8, int32, int64:
										if idx == len(r)-1 {
											file.WriteString(strconv.FormatInt(el.(int64), 10))
										} else {
											file.WriteString(strconv.FormatInt(el.(int64), 10) + ",")
										}
									case time.Time:
										if idx == len(r)-1 {
											file.WriteString(strconv.FormatInt(el.Unix(), 10))
										} else {
											file.WriteString(strconv.FormatInt(el.Unix(), 10) + ",")
										}
									default:
										log.Println("Unknown type.")
									}

								}
								file.WriteString("\n")

							case f := <-nextFile:

								if file != nil {
									file.Sync()
									file.Close()
									file = nil
								}

								fileName = f.fileName
								columns = f.columns

							case <-noMoreRows:

								if file != nil {
									file.Sync()
									file.Close()
									file = nil
								}

								break
							}
						}
						doneDumping <- true
					}()

					fileI := 0
					for results := true; results; results = rows.NextResultSet() {
						rowI := 0
						for rows.Next() {
							if rowI >= q.maxLines {
								rowI = 0
							}

							if rowI == 0 {
								columns, _ := rows.Columns()

								fileName := fmt.Sprintf("%s/%s/%03d.csv", q.outputDirPath, q.tableName, fileI)
								nextFile <- fileInfo{fileName: fileName, columns: columns}
								fileI++
							}

							row, _ := rows.SliceScan()
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
		psQuery, _ := DB.Database.Preparex(tbl.Query)
		queriesToExecute <- queryParams{stmt: psQuery, maxLines: tbl.MaxLines, tableName: tbl.Name, outputDirPath: conf.OutputDir}
	}
	noMoreQueries <- true

	for _, done := range dones {
		<-done
	}
}
