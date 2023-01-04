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
}

// dbname is one of `bdb.InitDB` names
func (o myObject) BadgerDB() *badger.DB {
	return bdb.DbGrp.DBs[""]
}

func (o myObject) ID() any {
	return o.Id
}

func (o *myObject) Unmarshal(fm map[string]any) error {

	if v, ok := FlatMapValTryToType[int](fm, "Id"); ok {
		o.Id = v
	} else {
		return fmt.Errorf("[Id] cannot be found or set as int")
	}

	if v, ok := FlatMapValTryToType[string](fm, "Wealth.Account"); ok {
		o.Wealth.Account = v
	} else {
		return fmt.Errorf("[Wealth.Account] cannot be found or set as int")
	}

	if v, ok := FlatMapValTryToType[float64](fm, "Wealth.Money"); ok {
		o.Wealth.Money = v
	} else {
		return fmt.Errorf("[Wealth.Money] cannot be found or set as int")
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
				Money:   1000,
			},
		},
		{
			Id: 234,
			Wealth: struct {
				Account string
				Money   float64
			}{
				Account: "ABCDEFG",
				Money:   2000,
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

	ids, err := bdb.GetIDs[myObject]("Money", 1000)
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
