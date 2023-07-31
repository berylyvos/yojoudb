package yojoudb

import (
	"os"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
}
