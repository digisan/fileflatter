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
	sep1 = []byte(":@")
	sep2 = []byte(":$")
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

// update or insert field
// func UpsertFieldInBadger[V any, T PtrBadgerAccessible[V]](object T, key string, val []byte) error {
// 	if _, ok := object.FlatMap()[key]; ok {
// 		id := object.ID()
// 		return object.BadgerDB().Update(func(txn *badger.Txn) error {

// 			// 1) key: id:path; val: val
// 			k1 := append(append(id, sep...), StrToConstBytes(key)...)
// 			v1 := val
// 			if err := txn.Set(k1, v1); err != nil {
// 				return err
// 			}

// 			// 2) KEY: val:rpath; VAL: id

// 			// 3) KEY: rpath; 	   VAL: id:val

// 			return nil
// 		})
// 	}
// 	return nil
// }

// update or insert object
func UpsertObjectInBadger[V any, T PtrBadgerAccessible[V]](object T) (string, error) {

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

		// 1) KEY: [id:@path] VAL: [val] --> Look for val
		k1 := append(append(idBuf, sep1...), pathBuf...)
		v1 := valBuf
		if err := wb.Set(k1, v1); err != nil {
			return "", err
		}

		// 2) KEY: [val:$rpath]; VAL: [id] --> None iter, accurate for id, then use this id for value
		if len(v1) <= 64 {
			k2 := append(append(v1, sep2...), rpathBuf...)
			v2 := idBuf
			if err := wb.Set(k2, v2); err != nil {
				return "", err
			}
		}

		// 3) KEY: [rpath]; VAL: [id:@val] --> Iter and look for id, then use this id for value
		k3 := rpathBuf
		v3 := append(append(idBuf, sep1...), v1...)
		if err := wb.Set(k3, v3); err != nil {
			return "", err
		}
	}
	return id, wb.Flush()
}

// Search
// one object with prefix `id:`
func GetObject[V any, T PtrBadgerAccessible[V]](id any) (T, error) {

	idBuf := StrToConstBytes(fmt.Sprint(id))
	if len(idBuf) == 0 {
		return nil, fmt.Errorf("[id] is empty, return undefined")
	}

	var (
		prefix = append(idBuf, sep1...)
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

// Delete
