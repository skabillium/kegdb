package keg

import (
	"encoding/gob"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

type KeyMetadata struct {
	Header Header
	offset int
	fileId int
}

type KeyDir map[string]KeyMetadata

type Keg struct {
	enableLogs   bool
	logr         *log.Logger
	dataDir      string
	snapshotFile string

	currentId int
	active    *Datafile
	stale     map[int]*Datafile
	keys      KeyDir
}

type KegOptions struct {
	DataDir     string
	LogsEnabled bool
}

func NewKegDB(options KegOptions) *Keg {
	return &Keg{
		keys:         KeyDir{},
		stale:        map[int]*Datafile{},
		dataDir:      options.DataDir,
		snapshotFile: options.DataDir + "/snapshot.gob",
		logr:         log.New(os.Stderr, "", log.Ldate|log.Ltime),
	}
}

func (k *Keg) Open() error {
	if !fileExists(k.dataDir) {
		err := os.Mkdir(k.dataDir, 0755)
		if err != nil {
			return err
		}
	}

	if fileExists(k.snapshotFile) {
		err := k.loadSnapshot()
		if err != nil {
			return err
		}
	} else {
		err := k.buildDbFromDatafiles()
		if err != nil {
			return err
		}
	}

	next := k.getNextFileId()
	active, err := NewDatafile(k.dataDir, next, false)
	if err != nil {
		return err
	}
	k.active = active

	return nil
}

func (k *Keg) Close() {
	k.active.Close()
	for _, df := range k.stale {
		df.Close()
	}
}

func (k *Keg) RunSnapshotJob(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		err := k.saveSnapshot()
		if err != nil {
			k.log("Error while saving snapshot:")
			k.log(err.Error())
		}

		k.log("Took snapshot")
	}
}

// Read all data files and rebuild indexes
func (k *Keg) buildDbFromDatafiles() error {
	files, err := k.listDataFiles()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	// Sort files based on id
	sort.Slice(files, func(i, j int) bool {
		return getFileIdFromName(files[i]) < getFileIdFromName(files[j])
	})

	k.log("Restoring database from %d data files...", len(files))
	for _, f := range files {
		id := getFileIdFromName(f)
		name := k.dataDir + "/" + f
		file, err := os.Open(name)
		if err != nil {
			return err
		}

		df, err := NewDatafile(k.dataDir, id, true)
		if err != nil {
			return err
		}

		k.stale[id] = df

		var offset int
		for {
			record, err := DecodeRecord(file)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			meta, found := k.keys[record.Key]
			if !found || (found && meta.Header.Timestamp < record.Header.Timestamp) {
				k.keys[record.Key] = KeyMetadata{Header: record.Header, offset: offset, fileId: id}
			}

			if record.Header.IsDeleted {
				delete(k.keys, record.Key)
			}

			offset += HeaderLength + int(record.Header.KeySize) + int(record.Header.ValueSize)
		}
	}

	return nil
}

// Take snapshot of keydir
func (k *Keg) saveSnapshot() error {
	snap, err := os.Create(k.snapshotFile)
	if err != nil {
		return err
	}
	defer snap.Close()

	encoder := gob.NewEncoder(snap)

	err = encoder.Encode(k.keys)
	if err != nil {
		return err
	}
	return nil
}

// Load index snapshots
func (k *Keg) loadSnapshot() error {
	k.log("Restoring from snapshot")
	snap, err := os.Open(k.snapshotFile)
	if err != nil {
		return err
	}
	defer snap.Close()

	decoder := gob.NewDecoder(snap)
	err = decoder.Decode(&k.keys)
	if err != nil {
		return err
	}
	return nil
}

// Write a record to the active file
func (k *Keg) writeRecord(rec *Record) error {
	offset, err := k.active.WriteRecord(rec)
	if err != nil {
		return err
	}
	k.keys[rec.Key] = KeyMetadata{Header: rec.Header, offset: offset, fileId: k.active.id}

	if k.active.HasExceededLimit() {
		err = k.switchActive()
		if err != nil {
			return err
		}
	}

	return nil
}

func (k *Keg) switchActive() error {
	k.active.CloseWriter()
	k.stale[k.active.id] = k.active
	act, err := NewDatafile(k.dataDir, k.getNextFileId(), false)
	if err != nil {
		return err
	}
	k.active = act
	return nil
}

func (k *Keg) getDatafile(id int) (*Datafile, bool) {
	if k.active.id == id {
		return k.active, true
	}

	df, found := k.stale[id]
	if found {
		return df, found
	}

	return nil, false
}

func (k *Keg) log(fm string, args ...any) {
	if k.enableLogs {
		k.logr.Printf(fm+"\n", args...)
	}
}
