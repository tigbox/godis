package godis

import (
	"github.com/tigbox/godis/datastruct/dict"
	"github.com/tigbox/godis/datastruct/lock"
)

func makeTestDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		locker:     lock.Make(lockerSize),
		addAof: func(line CmdLine) {

		},
	}
}
