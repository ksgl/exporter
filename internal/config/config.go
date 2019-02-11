package config

import (
	"io/ioutil"
	"log"
	"os"
)

// easyjson:json
type Configuration struct {
	Connector string   `json:"conn"`
	OutputDir string   `json:"output_dir"`
	Tables    []*Table `json:"tables"`
}

// easyjson:json
type Table struct {
	Name     string `json:"name"`
	Query    string `json:"query"`
	MaxLines int    `json:"max_lines"`
}

func Configure(filename string) *Configuration {
	path := "../" + filename
	config := &Configuration{}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Println("Can't read configuration file.")
		os.Exit(1)
	}

	err = config.UnmarshalJSON(data)
	if err != nil {
		log.Println("Can't unmarshal data into configuration file.")
		os.Exit(1)
	}

	return config
}
