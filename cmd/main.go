package main

import (
	"flag"
)

var (
	conf    = flag.String("config", "conf.json", "Configuration file")
	threads = flag.Int("threads", 1, "Thread count")
)

func main() {
	flag.Parse()

	return
}
