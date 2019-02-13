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

		go func(qs chan queryParams, nmw chan bool, dn chan bool) {
			for true {
				select {
				case q := <-qs:
					log.Println("qwery obtained")

					rows, _ := q.stmt.Queryx()
					columns, _ := rows.Columns()

					nextFileName := make(chan string)
					rowsToDump := make(chan []interface{})
					noMoreRows := make(chan bool)
					doneDumping := make(chan bool)

					go func(nfn chan string, rtd chan []interface{}, nmr chan bool, dD chan bool) {
						var file *os.File
						for true {
							select {
							case r := <-rtd:

								if file == nil {
									if f == "" {
										log.Fatal("File doesn't exist.")
									} else {
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

							case f := <-nfn:

								log.Println(f)
								if _, err := os.Stat(f); err != nil {
									os.MkdirAll(path.Dir(f), 0777)
								}

								if file != nil {
									file.Sync()
									file.Close()
								}

								file, _ = os.Create(f)
								file.Chmod(0777)

							case <-nmr:
								if file != nil {
									file.Sync()
									file.Close()
								}
								break
							}
						}
						dD <- true
					}(nextFileName, rowsToDump, noMoreRows, doneDumping)

					fileI := 0
					for results := true; results; results = rows.NextResultSet() {
						rowI := 0
						for rows.Next() {
							if rowI >= q.maxLines {
								rowI = 0
							}

							if rowI == 0 {
								fileName := fmt.Sprintf("%s/%s/%03d.csv", q.outputDirPath, q.tableName, fileI)
								nextFileName <- fileName
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

				case <-nmw:
					break
				}
			}
			done <- true
		}(queriesToExecute, noMoreQueries, done)
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
