package badgerdb

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	"github.com/google/uuid"
)

const (
	AsteriskDot = "*."
	HashDot     = "#."
)

var (
	SI      = []byte(":@")
	LenOfSI = len(SI)
	SV      = []byte(":$")
	LenOfSV = len(SV)
)

type BadgerAccessible interface {
	BadgerDB() *badger.DB
	ID() any
	Unmarshal(fm map[string]any) error
}

type PtrBadgerAccessible[T any] interface {
	BadgerAccessible
	*T
}

// update or insert an object
func UpsertObject[V any, T PtrBadgerAccessible[V]](object T) (string, error) {

	wb := T(new(V)).BadgerDB().NewWriteBatch()
	defer wb.Cancel()

	id := fmt.Sprint(object.ID())
	if len(id) == 0 {
		id = uuid.New().String()
	}

	fm, err := ObjsonToFlatMap(object)
	if err != nil {
		return "", err
	}
	if len(fm) == 0 {
		return "", nil
	}

	idBuf := StrToConstBytes(id)

	for path, val := range fm {

		pathBuf := StrToConstBytes(path)
		rpathBuf := StrToConstBytes(strs.ReversePath(path))
		valBuf := StrToConstBytes(fmt.Sprint(val))

		// 1) KEY: [id:@path] VAL: [val] --> fetch val
		//
		k := AppendBytes(idBuf, SI, pathBuf)
		v := valBuf
		if err := wb.Set(k, v); err != nil {
			return "", err
		}

		// 2) KEY: [val:$rpath:@id]; VAL: [] --> no iter, accurate for id, then use this id for value
		//
		if len(v) <= 36 {
			k = AppendBytes(valBuf, SV, rpathBuf, SI, idBuf)
			v = []byte{}
			if err := wb.Set(k, v); err != nil {
				return "", err
			}
		}

		// 3) KEY: [rpath:@id]; VAL: [val] --> Iter and look for id, then use this id for value
		//
		k = AppendBytes(rpathBuf, SI, idBuf)
		v = valBuf
		if err := wb.Set(k, v); err != nil {
			return "", err
		}
	}
	return id, wb.Flush()
}

// update or insert objects
func UpsertObjects[V any, T PtrBadgerAccessible[V]](objects ...T) ([]string, error) {
	ids := []string{}
	for _, object := range objects {
		id, err := UpsertObject(object)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// Search: one object with prefix `id:`
func GetObject[V any, T PtrBadgerAccessible[V]](id string) (T, error) {

	idBuf := StrToConstBytes(id)
	if len(idBuf) == 0 {
		return nil, fmt.Errorf("[id] is empty, return undefined")
	}

	var (
		prefix = append(idBuf, SI...)
		fm     = make(map[string]any)
		err    = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					key := item.Key()
					path := ConstBytesToStr(bytes.TrimPrefix(key, prefix))
					fm[path] = val
					return nil
				})
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := itemProc(it.Item()); err != nil {
					return err
				}
			}
			return nil
		})
	)
	if err != nil {
		return nil, err
	}

	if len(fm) == 0 {
		return nil, fmt.Errorf("object cannot be found @ [id] - %v", id)
	}

	rt := T(new(V))
	return rt, rt.Unmarshal(fm)
}

// Search: objects with prefix `id:`
func GetObjects[V any, T PtrBadgerAccessible[V]](ids ...string) (rt []T, err error) {
	for _, id := range ids {
		object, err := GetObject[V, T](id)
		if err != nil {
			return nil, err
		}
		rt = append(rt, object)
	}
	return rt, nil
}

// Search: get id group with (rpath, val) conditions
func GetID[V any, T PtrBadgerAccessible[V]](rpath string, val any) (ids []string, err error) {

	// 2) KEY: [val:$rpath:@id]; VAL: [] --> no iter, accurate for id, then use this id for value

	var (
		valBuf   = StrToConstBytes(fmt.Sprint(val))
		rpathBuf = StrToConstBytes(rpath)
		prefix   = AppendBytes(valBuf, SV, rpathBuf)
	)

	return ids, T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		itemProc := func(item *badger.Item) error {
			return item.Value(func(val []byte) error {
				key := item.Key()
				ids = append(ids, ConstBytesToStr(key[bytes.LastIndex(key, SI)+LenOfSI:]))
				return nil
			})
		}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err = itemProc(it.Item()); err != nil {
				return err
			}
		}
		return nil
	})
}

// rfm: map[rpath]value
func FetchIDByRP[V any, T PtrBadgerAccessible[V]](rfm map[string]any) ([]string, error) {
	idsGrp := [][]string{}
	for rpath, val := range rfm {
		ids, err := GetID[V, T](rpath, val)
		if err != nil {
			return nil, err
		}
		idsGrp = append(idsGrp, ids)
	}

	rt := []string{}
	idsMerged := SmashArrays(idsGrp...)
	for _, id := range SmashSets(idsGrp...) {
		if Count(idsMerged, id) == len(rfm) {
			rt = append(rt, id)
		}
	}
	return rt, nil
}

// fm: map[path]value
func FetchID[V any, T PtrBadgerAccessible[V]](fm map[string]any) ([]string, error) {
	rfm := make(map[string]any)
	for k, v := range fm {
		rfm[strs.ReversePath(k)] = v
	}
	return FetchIDByRP[V, T](rfm)
}

type FilterRP4Sgl func(rpath string, value []byte) bool
type FilterRP4Slc func(rpath string, idx int, value []byte) bool
type FilterRP4Map func(rpath string, key any, value []byte) bool

// [Object-Type]
func SearchIDByRPSgl[V any, T PtrBadgerAccessible[V]](prefixRP string, filter FilterRP4Sgl) (ids []string, err error) {

	// 3) KEY: [rpath:@id]; VAL: [val] --> Iter and look for id, then use this id for value

	var (
		prefixBuf = StrToConstBytes(prefixRP)
	)
	err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		itemProc := func(item *badger.Item) error {
			return item.Value(func(val []byte) error {
				var (
					key   = item.Key()
					id    = ConstBytesToStr(key[bytes.LastIndex(key, SI)+LenOfSI:])
					rpath = ConstBytesToStr(key[:bytes.Index(key, SI)])
				)
				if filter == nil || (filter != nil && filter(rpath, val)) {
					ids = append(ids, id)
				}
				return nil
			})
		}
		for it.Seek(prefixBuf); it.ValidForPrefix(prefixBuf); it.Next() {
			if err = itemProc(it.Item()); err != nil {
				return err
			}
		}
		return nil
	})

	return ids, err
}

// [Object-Type]
func SearchIDByRPSlc[V any, T PtrBadgerAccessible[V]](hashDotRP string, filter FilterRP4Slc, nCriteria int) (ids []string, err error) {

	// 3) KEY: [rpath:@id]; VAL: [val] --> Iter and look for id, then use this id for value

	if !strings.HasPrefix(hashDotRP, HashDot) {
		return nil, fmt.Errorf("[hashDotRP] must start with '%v'", HashDot)
	}
	if filter == nil {
		return nil, fmt.Errorf("[filter] cannot be nil")
	}

	var (
		mIdCnt = make(map[string]int)
	)

	rPathSlc := strings.TrimPrefix(hashDotRP, HashDot)
	restr := fmt.Sprintf(`^\d+.%s`, rPathSlc)
	restr = strings.ReplaceAll(restr, ".", "\\.")
	r := regexp.MustCompile(restr)

	err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		itemProc := func(item *badger.Item) error {
			return item.Value(func(val []byte) error {
				var (
					key   = item.Key()
					id    = ConstBytesToStr(key[bytes.LastIndex(key, SI)+LenOfSI:])
					rpath = ConstBytesToStr(key[:bytes.Index(key, SI)])
				)
				if !r.MatchString(rpath) {
					return nil
				}
				idx := 0
				if p := strings.Index(rpath, "."); p > -1 {
					if num, ok := AnyTryToType[int](rpath[:p]); ok {
						idx = num
					}
				}
				if filter != nil && filter(rpath, idx, val) {
					mIdCnt[id]++
				}
				return nil
			})
		}
		for it.Seek([]byte{}); it.ValidForPrefix([]byte{}); it.Next() {
			if err = itemProc(it.Item()); err != nil {
				return err
			}
		}
		return nil
	})

	for id, nc := range mIdCnt {
		if nc >= nCriteria {
			ids = append(ids, id)
		}
	}

	return ids, err
}

// [ByRPMap's Key-Type, Object-Type]
func SearchIDByRPMap[KT, V any, T PtrBadgerAccessible[V]](asteriskDotRP string, filter FilterRP4Map, nCriteria int) (ids []string, err error) {

	// 3) KEY: [rpath:@id]; VAL: [val] --> Iter and look for id, then use this id for value

	if !strings.HasPrefix(asteriskDotRP, AsteriskDot) {
		return nil, fmt.Errorf("[asteriskDotRP] must start with '%v'", AsteriskDot)
	}
	if filter == nil {
		return nil, fmt.Errorf("[filter] cannot be nil")
	}

	var (
		mIdCnt = make(map[string]int)
	)

	rPathSlc := strings.TrimPrefix(asteriskDotRP, AsteriskDot)
	restr := fmt.Sprintf(`^\w+.%s`, rPathSlc)
	restr = strings.ReplaceAll(restr, ".", "\\.")
	r := regexp.MustCompile(restr)

	err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		itemProc := func(item *badger.Item) error {
			return item.Value(func(val []byte) error {
				var (
					key   = item.Key()
					id    = ConstBytesToStr(key[bytes.LastIndex(key, SI)+LenOfSI:])
					rpath = ConstBytesToStr(key[:bytes.Index(key, SI)])
				)
				if !r.MatchString(rpath) {
					return nil
				}
				mapKey := *new(KT)
				if p := strings.Index(rpath, "."); p > -1 {
					if field, ok := AnyTryToType[KT](rpath[:p]); ok {
						mapKey = field
					}
				}
				if filter != nil && filter(rpath, mapKey, val) {
					mIdCnt[id]++
				}
				return nil
			})
		}
		for it.Seek([]byte{}); it.ValidForPrefix([]byte{}); it.Next() {
			if err = itemProc(it.Item()); err != nil {
				return err
			}
		}
		return nil
	})

	for id, nc := range mIdCnt {
		if nc >= nCriteria {
			ids = append(ids, id)
		}
	}

	return ids, err
}

// Delete
