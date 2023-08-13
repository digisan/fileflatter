package badgerdb

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	lk "github.com/digisan/logkit"
)

var (
	once  sync.Once // do once
	DbGrp *DBGrp    // global, for keeping single instance
)

type DBGrp struct {
	sync.Mutex
	DBs map[string]*badger.DB
}

func open(dir string) *badger.DB {
	opt := badger.DefaultOptions("").WithInMemory(true)
	if dir != "" {
		opt = badger.DefaultOptions(dir)
		opt.Logger = nil
	}
	db, err := badger.Open(opt)
	lk.FailOnErr("%v", err)
	return db
}

// init global 'DbGrp', if dir is empty, and no dbs, use memory mode
func InitDB(dir string, names ...string) *DBGrp {
	if DbGrp == nil {
		once.Do(func() {
			DbGrp = &DBGrp{
				DBs: make(map[string]*badger.DB),
			}
			for _, name := range names {
				DbGrp.DBs[name] = open(filepath.Join(dir, name))
			}
		})
	}
	return DbGrp
}

func CloseDB() {
	DbGrp.Lock()
	defer DbGrp.Unlock()

	for name, db := range DbGrp.DBs {
		if db != nil {
			lk.FailOnErr("%v", db.Close())
			DbGrp.DBs[name] = nil
		}
	}
}
