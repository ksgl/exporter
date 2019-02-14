package main

import (
	"exporter/internal/config"
	"exporter/internal/database"
	"exporter/internal/fill"
	"flag"
	"os"
)

var (
	confFile = flag.String("config", "conf.json", "Configuration file")
	threads  = flag.Int("threads", 1, "Thread count")
)

func main() {
	flag.Parse()
	conf := config.ReadConfiguration(*confFile)

	if _, err := os.Stat(conf.OutputDir); err == nil {
		os.RemoveAll(conf.OutputDir)
	}

	db := database.Connect(*conf)
	fill.Populate(db)
	db.ExportCSV(*conf, *threads)

	return
}
