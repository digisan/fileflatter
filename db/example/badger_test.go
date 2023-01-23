package example

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v3"
	bdb "github.com/digisan/fileflatter/db/badgerdb"
	. "github.com/digisan/go-generics/v2"
)

type myObject struct {
	Id     int
	Wealth struct {
		Account string
		Money   float64
	}
	Arr []int
	M   map[int]string
	MA  map[string][]int
}

var (
	objs = []myObject{
		{
			Id: 123,
			Wealth: struct {
				Account string
				Money   float64
			}{
				Account: "abcdefg",
				Money:   100,
			},
			Arr: []int{1, 2, 3},
			M: map[int]string{
				123: "m123",
				234: "m234",
			},
			MA: map[string][]int{
				"MA1": {111, 222, 333},
				"MA2": {11, 22, 33},
			},
		},
		{
			Id: 456,
			Wealth: struct {
				Account string
				Money   float64
			}{
				Account: "ABCDEFG",
				Money:   200,
			},
			Arr: []int{4, 5, 6},
			M: map[int]string{
				1234: "m1234",
				2345: "m2345",
			},
			MA: map[string][]int{
				"MA1": {44, 55, 66},
				"MA2": {444, 555, 666},
			},
		},
	}
)

// dbname is one of `bdb.InitDB` names
func (o myObject) BadgerDB() *badger.DB {
	return bdb.DbGrp.DBs["test1"] // "test1" is one of dbnames in 'InitDB(, ...dbname)'
}

func (o myObject) ID() any {
	return "" // o.Id
}

func (o *myObject) Unmarshal(fm map[string]any) error {

	// single primitive
	if err := FlatMapSetField[int](fm, o, "Id"); err != nil {
		return err
	}

	// primitives
	if err := FlatMapSetFieldAsSlc[int](fm, o, "Arr"); err != nil {
		return err
	}

	// map of primitive
	if err := FlatMapSetFieldAsMap[int, string](fm, o, "M"); err != nil {
		return err
	}

	// map value of primitives
	if err := FlatMapSetFieldAsSlcValMap[string, int](fm, o, "MA"); err != nil {
		return err
	}

	///////////////////////////////

	// nested primitive
	if v, ok := FlatMapValTryToType[string](fm, "Wealth.Account"); ok {
		o.Wealth.Account = v
	} else {
		return fmt.Errorf("[Wealth.Account] cannot be found or set as string")
	}

	// nested primitive
	if v, ok := FlatMapValTryToType[float64](fm, "Wealth.Money"); ok {
		o.Wealth.Money = v
	} else {
		return fmt.Errorf("[Wealth.Money] cannot be found or set as float64")
	}

	return nil
}

func TestDB(t *testing.T) {

	bdb.InitDB("./data", "test1")
	defer bdb.CloseDB()

	fmt.Printf("\noriginal objects: %+v\n\n", objs)

	// *** store single object ***
	//
	// id, err := bdb.UpsertObject(&objs[0])
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Printf("storage ids: %+v\n\n", id)

	// *** store objects ***
	//
	ids, err := bdb.UpsertObjects(SlcToPtrSlc(objs...)...)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("storage ids: %+v\n\n", ids)

	//////////////////////////////////////////////

	// *** load object ***
	//
	// objects, err := bdb.GetObjects[myObject](ids...)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Printf("%+v\n", PtrSlcToSlc(objects...))

	//////////////////////////////////////////////

	// *** search object id(s) ***
	//

	// ids, err = bdb.GetID[myObject]("Money", 100)

	// ids, err = bdb.FetchIDByRP[myObject](map[string]any{
	// 	"Id":    456,
	// 	"Money": 200,
	// })

	ids, err = bdb.SearchIDByRPSgl[myObject]("Id", func(rpath string, value []byte) bool {
		v, ok := AnyTryToType[int](value)
		return ok && v < 156
	})

	// ids, err = bdb.SearchIDByRPSlc[myObject]("#.MA1", func(rpath string, idx int, value []byte) bool {
	// 	if v, ok := AnyTryToType[int](value); ok {
	// 		switch idx {
	// 		case 0:
	// 			return v == 44
	// 		case 1:
	// 			return v == 55
	// 		case 2:
	// 			return v == 66
	// 		}
	// 	}
	// 	return false
	// }, 3)

	// ids, err = bdb.SearchIDByRPMap[string, myObject]("*.Wealth", func(rpath string, key any, value []byte) bool {
	// 	switch key {
	// 	case "Account":
	// 		if acc, ok := AnyTryToType[string](value); ok {
	// 			return acc == "abcdefg"
	// 		}
	// 	case "Money":
	// 		if money, ok := AnyTryToType[int](value); ok {
	// 			return money < 150
	// 		}
	// 	}
	// 	return false
	// }, 2)

	// ids, err = bdb.SearchIDByRPMap[int, myObject]("*.M", func(rpath string, key any, value []byte) bool {
	// 	switch key {
	// 	case 123:
	// 		if v, ok := AnyTryToType[string](value); ok {
	// 			return v == "m123"
	// 		}
	// 	case 234:
	// 		if v, ok := AnyTryToType[string](value); ok {
	// 			return v == "m234"
	// 		}
	// 	}
	// 	return false
	// }, 2)

	////////////////////////////////////////////////////////////////////////////////////

	if err != nil {
		fmt.Println(err)
		return
	}
	if len(ids) == 0 {
		fmt.Println("NOT Found")
	}

	// *** get object from its id ***
	//
	for _, id := range ids {
		objR, err := bdb.GetObject[myObject](id)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("search output: %+v\n", *objR)
	}
	fmt.Println()
}
