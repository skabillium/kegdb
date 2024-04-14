package keg

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

const DataDir = "data"
const SnapshotFile = DataDir + "/snapshot.gob"

const FileSizeLimit = 512 * 1024 // Set maximum file size to 512kb

type KeyMetadata struct {
	Header Header
	offset int
	fileId int
}

type KeyDir map[string]KeyMetadata

type Keg struct {
	currentId int
	active    *Datafile
	stale     map[int]*Datafile

	keys KeyDir
}

func NewKegDB() *Keg {
	return &Keg{keys: KeyDir{}, stale: map[int]*Datafile{}}
}

func (k *Keg) saveSnapshot() error {
	snap, err := os.Create(SnapshotFile)
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

func (k *Keg) buildDbFromDatafiles() error {
	files, err := listDataFiles()
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

	fmt.Printf("Restoring database from %d data files... \n", len(files))
	for _, f := range files {
		id := getFileIdFromName(f)
		name := DataDir + "/" + f
		file, err := os.Open(name)
		if err != nil {
			return err
		}

		df, err := NewDatafile(id, true)
		if err != nil {
			panic(err)
		}

		k.stale[id] = df

		var offset int
		for {
			record, err := DecodeRecord(file)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			// TODO: Refactor this
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

// Merge stale data files to one
func (k *Keg) Merge() error {
	tmp, err := os.OpenFile("tmp.db", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	var offset int
	k.currentId = 1
	for key, meta := range k.keys {
		df, found := k.getDatafile(meta.fileId)
		if !found {
			return fmt.Errorf("Could not find file '%s'", fmt.Sprintf(FileFmt, meta.fileId))
		}

		rec, err := df.ReadRecord(meta.offset)
		if err != nil {
			return err
		}

		encoded, err := rec.Encode()
		if err != nil {
			return err
		}

		n, err := tmp.Write(encoded)
		if err != nil {
			return err
		}

		meta.offset = offset
		meta.fileId = 1
		k.keys[key] = meta

		offset += n
	}

	tmp.Close()
	k.stale = make(map[int]*Datafile)
	k.active = nil
	k.currentId = 1

	err = os.RemoveAll(DataDir)
	if err != nil {
		return err
	}

	err = os.Mkdir(DataDir, 0755)
	if err != nil {
		return err
	}

	err = os.Rename("tmp.db", DataDir+"/keg-1.db")
	if err != nil {
		return err
	}

	first, err := NewDatafile(1, true)
	if err != nil {
		return err
	}

	k.stale[first.id] = first

	k.currentId++
	k.active, err = NewDatafile(k.currentId, false)
	if err != nil {
		return err
	}

	return nil
}

func (k *Keg) loadSnapshot() error {
	snap, err := os.Open(SnapshotFile)
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

func (k *Keg) Open() error {
	if !fileExists(DataDir) {
		err := os.Mkdir(DataDir, 0755)
		if err != nil {
			return err
		}
	}

	if fileExists(SnapshotFile) {
		fmt.Println("Snapshot file found")
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

	next := getNextFileId()
	active, err := NewDatafile(next, false)
	if err != nil {
		panic(err)
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

func (k *Keg) readKey(meta KeyMetadata, df *Datafile) (string, error) {
	buf, err := df.ReadAt(int(int64(meta.offset)+int64(HeaderLength)), int(meta.Header.KeySize))
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// Read a value from a datafile
func (k *Keg) readValue(meta KeyMetadata, df *Datafile) ([]byte, error) {
	buf, err := df.ReadAt(int(int64(meta.offset)+int64(HeaderLength)+int64(meta.Header.KeySize)), int(meta.Header.ValueSize))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// Write a record to the active file
func (k *Keg) writeRecord(rec *Record) error {
	encoded, err := rec.Encode()
	if err != nil {
		return err
	}

	// TODO: Handle case of key bigger than FileSizeLimit
	if !k.active.HasCapacity(len(encoded)) {
		k.active.CloseWriter()
		k.stale[k.active.id] = k.active

		k.currentId++
		active, err := NewDatafile(k.currentId, false)
		if err != nil {
			panic(err) // TODO: Handle this differently
		}
		k.active = active
	}
	offset := k.active.Write(encoded)
	k.keys[rec.Key] = KeyMetadata{Header: rec.Header, offset: offset}

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

func (k *Keg) RunSnapshotJob(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		err := k.saveSnapshot()
		if err != nil {
			fmt.Println("Error while saving snapshot:")
			fmt.Println(err)
		}

		fmt.Println("Took snapshot")
	}
}
