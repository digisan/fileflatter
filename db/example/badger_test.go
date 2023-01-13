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
				"MA2": {444, 555},
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
			Arr: []int{1, 2, 3, 4},
			M: map[int]string{
				1234: "m1234",
				2345: "m2345",
			},
			MA: map[string][]int{
				"MA1": {666, 777, 888},
				"MA2": {999, 111},
			},
		},
	}
)

// dbname is one of `bdb.InitDB` names
func (o myObject) BadgerDB() *badger.DB {
	return bdb.DbGrp.DBs[""]
}

func (o myObject) ID() any {
	return o.Id
}

func (o *myObject) Unmarshal(fm map[string]any) error {

	// simple primitive
	if err := FlatMapSetField[int](fm, o, "Id"); err != nil {
		return err
	}

	// array of primitive
	if err := FlatMapSetFieldAsSlc[int](fm, o, "Arr"); err != nil {
		return err
	}

	// map of primitive
	if err := FlatMapSetFieldAsMap[int, string](fm, o, "M"); err != nil {
		return err
	}

	// map value of primitive array
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

func TestUpdate(t *testing.T) {

	bdb.InitDB("", "")
	defer bdb.CloseDB()

	fmt.Printf("%+v\n", objs)

	ids, err := bdb.UpsertObjects(SlcToPtrSlc(objs...)...)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(ids)

	//////////////////////////////////////////////

	objects, err := bdb.GetObjects[myObject](ids...)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v\n", PtrSlcToSlc(objects...))

	//////////////////////////////////////////////

	// ids, err := bdb.GetIDs[myObject]("Money", 1000)

	ids, err = bdb.FetchIDsRP[myObject](map[string]any{
		"Id":    456,
		"Money": 200,
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
