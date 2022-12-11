package fileflatter

import (
	"fmt"
	"strings"

	. "github.com/digisan/go-generics/v2"
)

func GetCSVObjects(fm map[string]any, indices ...int) (rt []map[string]any) {
	rt = make([]map[string]any, len(indices))
	for i := 0; i < len(indices); i++ {
		rt[i] = make(map[string]any)
	}
	keys, _ := MapToKVs(fm, nil, nil)
	for i, idx := range indices {
		for _, key := range keys {
			suffix := fmt.Sprintf(".%d", idx)
			if strings.HasSuffix(key, suffix) {
				newKey := strings.TrimSuffix(key, suffix)
				rt[i][newKey] = fm[key]
			}
		}
	}
	return
}
