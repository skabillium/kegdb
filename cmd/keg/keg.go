package keg

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DataDir = "data"
const SnapshotFile = DataDir + "/snapshot.gob"

const FileSizeLimit = 512 * 1024 // Set maximum file size to 512kb

type Keg struct {
	currentId int
	active    *Datafile
	stale     map[int]*Datafile

	offset int
	keys   KeyDir
}

func NewKegDB() *Keg {
	return &Keg{keys: KeyDir{}, stale: map[int]*Datafile{}}
}

func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

func listDataFiles() ([]string, error) {
	regex, err := regexp.Compile(`keg-(\d+).db`)
	if err != nil {
		return nil, err
	}

	files := []string{}
	err = filepath.Walk(DataDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := info.Name()
		if !info.IsDir() && regex.MatchString(name) {
			files = append(files, name)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

func now() uint32 {
	return uint32(time.Now().Unix())
}

func getNextFileId() int {
	ids := []int{}

	files, err := listDataFiles()
	if err != nil {
		panic(err)
	}

	if len(files) == 0 {
		return 1
	}

	for _, f := range files {
		split := strings.Split(f, ".")
		id, err := strconv.Atoi(split[0][len("keg-"):])
		if err != nil {
			panic(err)
		}

		ids = append(ids, id)
	}

	sort.Ints(ids)

	return ids[len(ids)-1] + 1
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

func getFileIdFromName(name string) int {
	split := strings.Split(name, ".")
	id, err := strconv.Atoi(split[0][len("keg-"):])
	if err != nil {
		return -1
	}
	return id
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

		df, err := NewDatafile(id)
		if err != nil {
			panic(err)
		}
		df.CloseWriter()

		k.stale[id] = df

		var offset int
		reader := bufio.NewReader(file)
		for {
			record, err := DecodeRecord(reader)
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
	active, err := NewDatafile(next)
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

func (k *Keg) Reindex() error {
	return nil
}

func (k *Keg) Set(key string, value string) error {
	rec := NewRecord(key, []byte(value), uint32(time.Now().Unix()))
	err := k.writeRecord(rec)
	if err != nil {
		return err
	}

	return nil
}

func (k *Keg) writeRecord(rec *Record) error {
	encoded, err := rec.Encode()
	if err != nil {
		return err
	}

	// TODO: Handle case of key bigger than FileSizeLimit
	if !k.active.HasCapacity(len(encoded)) {
		k.stale[k.active.id] = k.active
		k.currentId++
		active, err := NewDatafile(k.currentId)
		if err != nil {
			panic(err) // TODO: Handle this differently
		}
		k.active = active
	}
	offset := k.active.Write(encoded)
	k.keys[rec.Key] = KeyMetadata{Header: rec.Header, offset: offset}

	return nil
}

func (k *Keg) Get(key string) (string, error) {
	meta, found := k.keys[key]
	if !found {
		return "", nil
	}

	var df *Datafile
	df, found = k.stale[meta.fileId]
	if !found {
		if meta.fileId == k.active.id {
			df = k.active
		} else {
			return "", nil
		}
	}

	buf, err := df.ReadAt(int(int64(meta.offset)+int64(HeaderLength)+int64(meta.Header.KeySize)), int(meta.Header.ValueSize))
	if err != nil {
		return "", err
	}

	// TODO: Checksum

	return string(buf), nil
}

func (k *Keg) Delete(key string) (bool, error) {
	// Write a tombstone
	_, found := k.keys[key]
	if !found {
		return false, nil
	}

	rec := NewRecord(key, []byte{}, now())
	rec.Header.IsDeleted = true
	err := k.writeRecord(rec)
	if err != nil {
		return true, err
	}
	delete(k.keys, key)

	return true, nil
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
