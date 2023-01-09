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
	M   map[string]string
	MA  map[string][]int
}

// dbname is one of `bdb.InitDB` names
func (o myObject) BadgerDB() *badger.DB {
	return bdb.DbGrp.DBs[""]
}

func (o myObject) ID() any {
	return "" // o.Id
}

func (o *myObject) Unmarshal(fm map[string]any) error {

	// simple primitive
	if v, ok := FlatMapValTryToType[int](fm, "Id"); ok {
		o.Id = v
	} else {
		return fmt.Errorf("[Id] cannot be found or set as int")
	}

	// nested primitive
	if v, ok := FlatMapValTryToType[string](fm, "Wealth.Account"); ok {
		o.Wealth.Account = v
	} else {
		return fmt.Errorf("[Wealth.Account] cannot be found or set as string")
	}

	if v, ok := FlatMapValTryToType[float64](fm, "Wealth.Money"); ok {
		o.Wealth.Money = v
	} else {
		return fmt.Errorf("[Wealth.Money] cannot be found or set as float64")
	}

	// array of primitive
	if v, ok := FlatMapValsTryToTypes[int](fm, "Arr"); ok {
		o.Arr = v
	} else {
		return fmt.Errorf("[Arr] cannot be found or set as int")
	}

	// map value of primitive
	if v, ok := FlatMapValTryToType[string](fm, "M.M1"); ok {
		if len(o.M) == 0 {
			o.M = make(map[string]string)
		}
		o.M["M1"] = v
	} else {
		return fmt.Errorf("[Arr] cannot be found or set as int")
	}

	// map value of primitive array
	mapField := "MA"
	for _, key := range FlatMapSubKeys(fm, mapField) {
		path := mapField + "." + key
		if v, ok := FlatMapValsTryToTypes[int](fm, path); ok {
			if len(o.MA) == 0 {
				o.MA = make(map[string][]int)
			}
			o.MA[key] = v
		} else {
			return fmt.Errorf("[MA] cannot be found or set as int array")
		}
	}

	return nil
}

func TestUpdate(t *testing.T) {

	bdb.InitDB("", "")
	defer bdb.CloseDB()

	objs := []myObject{
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
			M: map[string]string{
				"M1": "m123",
			},
			MA: map[string][]int{
				"MA1": {111, 222, 333},
				"MA2": {444, 555},
			},
		},
		{
			Id: 123,
			Wealth: struct {
				Account string
				Money   float64
			}{
				Account: "ABCDEFG",
				Money:   200,
			},
			Arr: []int{1, 2, 3, 4},
			M: map[string]string{
				"M1": "m1234",
			},
			MA: map[string][]int{
				"MA1": {666, 777, 888},
				"MA2": {999, 111},
			},
		},
	}
	fmt.Printf("%+v\n", objs)

	var err error

	id0 := ""
	if id0, err = bdb.UpsertObject(&objs[0]); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(id0)

	id1 := ""
	if id1, err = bdb.UpsertObject(&objs[1]); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(id1)

	//////////////////////////////////////////////

	// objR0, err := bdb.GetObject[myObject](id0)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Printf("%+v\n", *objR0)

	// objR1, err := bdb.GetObject[myObject](id1)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Printf("%+v\n", *objR1)

	//////////////////////////////////////////////

	// ids, err := bdb.GetIDs[myObject]("Money", 1000)

	ids, err := bdb.FetchIDsRP[myObject](map[string]any{
		"Id":    123,
		"Money": 100,
	})

	if err != nil {
		fmt.Println(err)
		return
	}
	if len(ids) == 0 {
		fmt.Println("NOT Found")
	}
	for _, id := range ids {
		objR, err := bdb.GetObject[myObject](id)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%+v\n", *objR)
	}

}
