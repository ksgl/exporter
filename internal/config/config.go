package config

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
