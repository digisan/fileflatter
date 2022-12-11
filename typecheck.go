package fileflatter

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"io"
	"os"

	"github.com/BurntSushi/toml"
	. "github.com/digisan/go-generics/v2"
	"github.com/h2non/filetype"
	"gopkg.in/yaml.v3"
)

// IsXML: Check str is valid XML
func IsXML(data []byte) bool {
	return xml.Unmarshal(data, new(any)) == nil
}

// IsJSON: Check str is valid JSON
func IsJSON(data []byte) bool {
	return json.Unmarshal(data, new(any)) == nil
}

// IsCSV: Check str is valid CSV
func IsCSV(data []byte) bool {
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	return err == nil && len(records) > 0
}

// IsTOML: check str is valid TOML
func IsTOML(data []byte) bool {
	m := map[string]any{}
	_, err := toml.Decode(ConstBytesToStr(data), &m)
	return err == nil && len(m) > 0
}

// IsYAML: check str is valid YAML
func IsYAML(data []byte) bool {
	m := map[string]any{}
	err := yaml.Unmarshal(data, &m)
	return err == nil && len(m) > 0
}

const (

	// Binary Type
	Document    = "document"
	Image       = "image"
	Audio       = "audio"
	Video       = "video"
	Archive     = "archive"
	Application = "executable"
	Binary      = "binary"

	// Text Type
	JSON = "json"
	XML  = "xml"
	CSV  = "csv"
	TOML = "toml"
	YAML = "yaml"

	Unknown = "unknown"
)

func BinType(f io.ReadSeeker) string {
	defer func() {
		f.Seek(0, io.SeekStart)
	}()
	head := make([]byte, 261)
	f.Read(head)
	switch {
	case filetype.IsImage(head):
		return Image
	case filetype.IsVideo(head):
		return Video
	case filetype.IsAudio(head):
		return Audio
	case filetype.IsDocument(head):
		return Document
	case filetype.IsArchive(head):
		return Archive
	case filetype.IsApplication(head):
		return Application
	default:
		return Unknown
	}
}

type Block interface {
	string | []byte | *os.File
}

func txtType(data []byte) string {
	switch {
	case IsJSON(data):
		return JSON
	case IsTOML(data):
		return TOML
	case IsXML(data):
		return XML
	case IsYAML(data):
		return YAML
	case IsCSV(data):
		return CSV
	default:
		return Unknown
	}
}

func TxtType[T Block](data T) string {
	var in any = data
	switch TypeOf(data) {
	case "string":
		return txtType(StrToConstBytes(in.(string)))
	case "[]uint8":
		return txtType(in.([]byte))
	case "*os.File":
		f := in.(*os.File)
		defer func() { f.Seek(0, io.SeekStart) }()
		bytes, err := io.ReadAll(f)
		if err != nil {
			return err.Error()
		}
		return txtType(bytes)
	}
	panic("shouldn't be here")
}
