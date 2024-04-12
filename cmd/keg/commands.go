package keg

import "time"

func (k *Keg) Reindex() error {
	return nil
}

func (k *Keg) Keys() ([]string, error) {
	return nil, nil
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

	buf, err := df.ReadAt(int(int64(meta.offset)+int64(HeaderLength)+int64(meta.Header.KeySize)), int(meta.Header.ValueSize))
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
