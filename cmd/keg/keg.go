package keg

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"
)

const DataDir = "data"
const SnapshotFile = DataDir + "/snapshot.gob"
const DatabaseFile = DataDir + "/active.keg"

type Keg struct {
	writer *os.File
	reader *os.File

	offset int
	keys   KeyDir
}

func NewKegDB() *Keg {
	return &Keg{keys: KeyDir{}}
}

func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

func (k *Keg) saveSnapshot() error {
	snap, err := os.Create("data/snapshot.gob")
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

func (k *Keg) loadSnapshot() error {
	snap, err := os.Open("data/snapshot.gob")
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

	write, err := os.OpenFile("data/active.keg", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	k.writer = write

	read, err := os.Open("data/active.keg")
	if err != nil {
		return err
	}
	k.reader = read

	if fileExists(SnapshotFile) {
		fmt.Println("Snapshot file found")
		err = k.loadSnapshot()
		if err != nil {
			return err
		}
	}

	return nil
}

func (k *Keg) Close() {
	k.writer.Close()
	k.reader.Close()
}

func (k *Keg) Set(key string, value string) error {
	record := NewRecord(key, []byte(value), uint32(time.Now().Unix()))
	encoded, err := record.Encode()
	if err != nil {
		return err
	}

	k.writer.Write(encoded)

	k.keys[key] = KeyMetadata{offset: k.offset, Header: record.Header}

	k.offset += len(encoded)
	return nil
}

func (k *Keg) Get(key string) (string, error) {
	meta, found := k.keys[key]
	if !found {
		return "", nil
	}

	_, err := k.reader.Seek(int64(meta.offset)+HeaderLength+int64(meta.Header.KeySize), 0)
	if err != nil {
		return "", err
	}

	buffer := make([]byte, meta.Header.ValueSize)
	_, err = k.reader.Read(buffer)
	if err != nil {
		return "", err
	}

	// TODO: Checksum

	return string(buffer), nil
}

func (k *Keg) Delete(key string) {
	delete(k.keys, key)
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
