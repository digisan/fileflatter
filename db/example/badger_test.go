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
	return "" //o.Id
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

	obj := myObject{
		Id: 123,
		Wealth: struct {
			Account string
			Money   float64
		}{
			Account: "abcdefg",
			Money:   1000,
		},
	}
	fmt.Printf("%+v\n", obj)

	id := ""
	var err error

	if id, err = bdb.UpsertObjectInBadger(&obj); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(id)

	obj1, err := bdb.GetObject[myObject](id)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v\n", *obj1)
}
