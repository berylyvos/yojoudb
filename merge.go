package yojoudb

import (
	"path"
	"path/filepath"
)

const (
	mergeDirName = "-merge"
	mergeFinKey  = "merged-before-fid"
)

func (db *DB) Merge() error {

	return nil
}

func (db *DB) loadMergedFiles() error {

	return nil
}

func (db *DB) loadIndexerFromHint() error {

	return nil
}

func (db *DB) getNotMergedFileId(dirPath string) (uint32, error) {
	return 0, nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}
