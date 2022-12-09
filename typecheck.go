package fileflatter

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"strings"

	"github.com/BurntSushi/toml"
)

// IsXML : Check str is valid XML
func IsXML(str string) bool {
	return xml.Unmarshal([]byte(str), new(any)) == nil
}

// IsJSON : Check str is valid JSON
func IsJSON(str string) bool {
	return json.Unmarshal([]byte(str), new(any)) == nil
}

// IsCSV : Check str is valid CSV
func IsCSV(str string) bool {
	records, err := csv.NewReader(strings.NewReader(str)).ReadAll()
	return err == nil && len(records) > 0
}

// IsTOML : check str is valid TOML
func IsTOML(str string) bool {
	m := map[string]any{}
	_, err := toml.Decode(str, &m)
	return err == nil && len(m) > 0
}
