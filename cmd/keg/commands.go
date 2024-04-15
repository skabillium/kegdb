package keg

import (
	"fmt"
	"os"
	"time"
)

func (k *Keg) Keys() []string {
	keys := []string{}
	for key := range k.keys {
		keys = append(keys, key)
	}
	return keys
}

func (k *Keg) Put(key string, value string) error {
	rec := NewRecord(key, []byte(value), uint32(time.Now().Unix()))
	err := k.writeRecord(rec)
	if err != nil {
		return err
	}

	return nil
}

func (k *Keg) Get(key string) (string, bool, error) {
	meta, found := k.keys[key]
	if !found {
		return "", false, nil
	}

	var df *Datafile
	df, found = k.stale[meta.fileId]
	if !found {
		if meta.fileId == k.active.id {
			df = k.active
		} else {
			return "", false, nil
		}
	}

	buf, err := df.ReadValue(meta)
	if err != nil {
		return "", true, err
	}

	// TODO: Checksum

	return string(buf), true, nil
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

func (k *Keg) Index() error {
	k.Close()
	k.buildDbFromDatafiles()
	return nil
}

// Merge stale data files to one
func (k *Keg) Merge() error {
	tempName := "keg-tmp.db"
	tmp, err := os.OpenFile(tempName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	k.currentId = 1
	var offset int
	for key, meta := range k.keys {
		df, found := k.getDatafile(meta.fileId)
		if !found {
			return fmt.Errorf("Could not find file '%s'", fmt.Sprintf(k.dataDir+"/keg-%d.db", meta.fileId))
		}

		rec, err := df.ReadRecord(meta.offset)
		if err != nil {
			return err
		}

		// TODO: Remove this
		if rec.Header.IsDeleted {
			continue
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
		meta.fileId = k.currentId
		k.keys[key] = meta

		offset += n
	}

	tmp.Close()
	k.stale = make(map[int]*Datafile)
	k.active = nil

	err = os.RemoveAll(k.dataDir)
	if err != nil {
		return err
	}

	err = os.Mkdir(k.dataDir, 0755)
	if err != nil {
		return err
	}

	err = os.Rename(tempName, k.dataDir+"/keg-1.db")
	if err != nil {
		return err
	}

	first, err := NewDatafile(k.dataDir, 1, true)
	if err != nil {
		return err
	}

	k.stale[first.id] = first

	k.currentId++
	k.active, err = NewDatafile(k.dataDir, k.currentId, false)
	if err != nil {
		return err
	}

	return nil
}
