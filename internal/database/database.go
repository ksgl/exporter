package database

import (
	"exporter/internal/config"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/xwb1989/sqlparser"
)

type DB struct {
	Database *sqlx.DB
}

type StmtMaxLines struct {
	stmt *sqlx.Stmt
	maxLines int
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
	// создаем папку results, если её не существует
	outputDirPath := conf.OutputDir
	if _, err := os.Stat(outputDirPath); err != nil {
		os.Mkdir(outputDirPath, 0777)
	}

	queriesToExecute := make(chan StmtMaxLines)
	noMoreQueries := make(chan bool)
	var dones []chan bool

	for i := 0; i < threads; i++ {
		done := make(chan bool)
		dones = append(dones, done)

		go func(qs chan StmtMaxLines, nmw chan bool, dn chan bool) {
		  for true {
			select {
			case q := <-qs:

			  rows, _ := q.Queryx()
			  columns, _ := rows.Columns()
			  columns = columns

			  nextFileName := make(chan string)
			  rowsToDump := make(chan []interface{}, 100)
			  noMoreRows := make(chan bool)
			  doneDumping := make(chan bool)

			  go func(nfn chan string, rtd chan []interface{}, nmr chan bool, dD chan bool) {
				for true {
				  select {
				  case r := <-rtd:
				  case <-nmr:
					break
				  }
				}
				dD <- true
			  }(nextFileName, rowsToDump, noMoreRows, doneDumping)

			  for results := true; results; results = rows.NextResultSet(){
				nextFileName <- /// file name + .000
			for rows.Next() {
				row, _ := rows.SliceScan()
				j := j + 1
				if j >= tbl {
				nextFileName <- /// file name + .001
				j= 0
				}
		
	
	
			rowsToDump <- row
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
		queriesToExecute <- StmtMaxLines{ stmt: psQuery, maxLines : tbl.MaxLines }
	}
	noMoreQueries <- true

	for _, done := range dones {
		<-done
	}

	// // для таблицы создаем папку, если её не существует
	// tableOutputDirPath := outputDirPath + "/" + table
	// if _, err := os.Stat(tableOutputDirPath); err != nil {
	// 	os.Mkdir(tableOutputDirPath, 0777)
	// }

	// // получаем данные селекта и размер данных
	// data, dataSize, rowSize := DB.execSelectQuery(tbl.Query)

	// // определяем количество файлов
	// filesCount := 1
	// if tbl.MaxLines < dataSize {
	// 	filesCount = int(math.Ceil(float64(dataSize) / float64(tbl.MaxLines)))
	// }

	// // создаём файлы и записываем в них данные
	// for i := 0; i < filesCount; i++ {
	// 	fileName := fmt.Sprintf("%s/%03d.csv", tableOutputDirPath, i+1)
	// 	file, _ := os.Create(fileName)
	// 	file.Chmod(0777)

	// 	for idx, col := range columns {
	// 		if idx == colSize-1 {
	// 			file.WriteString(col)
	// 		} else {
	// 			file.WriteString(col + ",")
	// 		}
	// 	}
	// 	file.WriteString("\n")

	// 	for _, row := range data {
	// for idx, el := range row {
	// 	switch el := el.(type) {
	// 	case string:
	// 		if idx == rowSize-1 {
	// 			file.WriteString(el)
	// 		} else {
	// 			file.WriteString(el + ",")
	// 		}
	// 	case int, int8, int32, int64:
	// 		if idx == rowSize-1 {
	// 			file.WriteString(strconv.FormatInt(el.(int64), 10))
	// 		} else {
	// 			file.WriteString(strconv.FormatInt(el.(int64), 10) + ",")
	// 		}
	// 	case time.Time:
	// 		if idx == rowSize-1 {
	// 			file.WriteString(strconv.FormatInt(el.Unix(), 10))
	// 		} else {
	// 			file.WriteString(strconv.FormatInt(el.Unix(), 10) + ",")
	// 		}
	// 	default:
	// 		log.Println("Unknown type.")
	// 	}
	// }
	// file.WriteString("\n")
	// 	}

	// 	file.Close()
	// }
	// }
}

// функция возвращает запрашиваемые селектом колонки
func parseSelectQuery(query string) (columns []string, colSize int) {
	stmt, _ := sqlparser.Parse(query)

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		for _, col := range stmt.SelectExprs {
			columns = append(columns, sqlparser.String(col))
		}
	}

	return columns, len(columns)
}
