package fileflatter

import (
	"fmt"
	"strings"

	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
)

func GetCSVObjects(fm map[string]any, indices ...int) (rt []map[string]any) {

	keys, _ := MapToKVs(fm, nil, nil)

	idxSuffix := func(key string) int {
		return strs.SplitPartFromLastTo[int](key, ".", 1)
	}

	fillVal := func(p, idx int) {
		rt[p] = make(map[string]any)
		for _, key := range keys {
			suffix := fmt.Sprintf(".%d", idx)
			if strings.HasSuffix(key, suffix) {
				newKey := strings.TrimSuffix(key, suffix)
				rt[p][newKey] = fm[key]
			}
		}
	}

	if len(indices) == 0 {
		suffixIndices := make([]int, 0, len(keys))
		for _, key := range keys {
			suffixIndices = append(suffixIndices, idxSuffix(key))
		}
		indices = IterToSlc(Max(suffixIndices...) + 1)
	}

	rt = make([]map[string]any, len(indices))
	for i, idx := range indices {
		fillVal(i, idx)
	}

	return
}
