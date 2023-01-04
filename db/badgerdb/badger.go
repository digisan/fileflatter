package badgerdb

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	. "github.com/digisan/go-generics/v2"
	"github.com/digisan/gotk/strs"
	"github.com/google/uuid"
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

// update or insert object
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
		k := AppendBytes(idBuf, SI, pathBuf)
		v := valBuf
		if err := wb.Set(k, v); err != nil {
			return "", err
		}

		// 2) KEY: [val:$rpath:@id]; VAL: [] --> no iter, accurate for id, then use this id for value
		if len(v) <= 64 {
			k = AppendBytes(valBuf, SV, rpathBuf, SI, idBuf)
			v = []byte{}
			if err := wb.Set(k, v); err != nil {
				return "", err
			}
		}

		// 3) KEY: [rpath:@id]; VAL: [val] --> Iter and look for id, then use this id for value
		k = AppendBytes(rpathBuf, SI, idBuf)
		v = valBuf
		if err := wb.Set(k, v); err != nil {
			return "", err
		}
	}
	return id, wb.Flush()
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

// Search: get id group with m map conditions
func GetIDs[V any, T PtrBadgerAccessible[V]](rpath string, val any) (ids []string, err error) {

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

// func SearchIDs

// Delete
