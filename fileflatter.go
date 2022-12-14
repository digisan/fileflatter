package fileflatter

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"

	"github.com/BurntSushi/toml"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	"gopkg.in/yaml.v3"
)

func flatContent(data []byte) (map[string]any, error) {

	var (
		object = make(map[string]any)
		err    error
	)

	switch TxtType(data) {
	case JSON:
		if err = json.Unmarshal(data, &object); err != nil {
			return nil, err
		}

	case TOML:
		if _, err = toml.Decode(string(data), &object); err != nil {
			return nil, err
		}

	case YAML:
		if err = yaml.Unmarshal(data, &object); err != nil {
			return nil, err
		}

	case XML:
		if err = xml.Unmarshal(data, &object); err != nil {
			return nil, err
		}

	case CSV:
		records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
		if err != nil {
			return nil, err
		}
		mIColHdr := make(map[int]string)
		for i, row := range records {
			// fmt.Printf("%+v\n", row)
			switch i {
			case 0: // headers
				for iCol, hdr := range row {
					object[hdr] = []any{}
					mIColHdr[iCol] = hdr
				}
			default: // rows
				for iCol, item := range row {
					hdr := mIColHdr[iCol]
					object[hdr] = append(object[hdr].([]any), item)
				}
			}
		}

	default:
		panic("unknown data type in [flatContent]")
	}

	return MapNestedToFlat(object), err
}

func FlatContent[T Block](data T) (map[string]any, error) {
	var in any = data
	switch TypeOf(data) {
	case "string":
		return flatContent(StrToConstBytes(in.(string)))
	case "[]uint8":
		return flatContent(in.([]byte))
	case "*os.File":
		f := in.(*os.File)
		defer func() { f.Seek(0, io.SeekStart) }()
		bytes, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		return flatContent(bytes)
	}
	panic("shouldn't be here")
}

func PrintFlat(m map[string]any) {
	ks, _ := MapToKVs(m, nil, nil)
	ks = strs.SortPaths(ks...)
	fmt.Println("\n------------------------------------")
	for _, k := range ks {
		keyLen := len(k)
		keySpace := (keyLen/10 + 1) * 10
		fmtstr := fmt.Sprintf("%%-%dv %%v\n", keySpace)
		fmt.Printf(fmtstr, k+":", m[k])
	}
	fmt.Println("------------------------------------")
}
