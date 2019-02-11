package main

import (
	"exporter/internal/config"
	"exporter/internal/database"
	"exporter/internal/fill"
	"flag"
)

var (
	confFile = flag.String("config", "conf.json", "Configuration file")
	threads  = flag.Int("threads", 1, "Thread count")
)

func main() {
	flag.Parse()
	conf := config.Configure(*confFile)

	db := database.Connect(*conf)
	fill.Populate(db)
	db.Export(*conf)

	return
}
