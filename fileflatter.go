package fileflatter

import (
	"encoding/json"

	"github.com/BurntSushi/toml"
	. "github.com/digisan/go-generics/v2"
)

func TxtType(data []byte) string {

	str := string(data)

	switch {
	case IsJSON(str):
		return "json"

	case IsTOML(str):
		return "toml"

	case IsXML(str):
		return "xml"

	case IsCSV(str):
		return "csv"

	default:
		return "unknown"
	}
}

func FlatContent(data []byte) (map[string]any, error) {

	var (
		str    = string(data)
		object = make(map[string]any)
		reFlat map[string]any
		err    error
	)

	t := FileType(data)

	switch {
	case IsJSON(str):
		if err = json.Unmarshal([]byte(str), &object); err != nil {
			return nil, err
		}
		reFlat = MapNestedToFlat(object)

	case IsTOML(str):
		m := map[string]any{}
		_, err := toml.Decode(str, &m)
		return err == nil && len(m) > 0

	case IsXML(str):

	case IsCSV(str):

	default:

	}

	return reFlat, err
}
