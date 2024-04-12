package keg

import (
	"fmt"
	"os"
)

const FileFmt = DataDir + "/keg-%d.db"

type Datafile struct {
	reader *os.File
	writer *os.File

	offset int
	id     int
}

func NewDatafile(id int, stale bool) (*Datafile, error) {
	datafile := &Datafile{}

	filepath := fmt.Sprintf(FileFmt, id)

	if !stale {
		write, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		datafile.writer = write
	}

	reader, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	datafile.reader = reader

	return datafile, nil
}

func (d *Datafile) Write(b []byte) int {
	d.writer.Write(b)
	offset := d.offset
	d.offset += len(b)

	return offset
}

func (d *Datafile) ReadAt(offset int, length int) ([]byte, error) {
	var buffer []byte
	_, err := d.reader.Seek(int64(offset), 0)
	if err != nil {
		return buffer, err
	}

	buffer = make([]byte, length)
	_, err = d.reader.Read(buffer)
	if err != nil {
		return buffer, err
	}

	return buffer, nil
}

func (d *Datafile) HasCapacity(n int) bool {
	return d.offset+n <= FileSizeLimit
}

func (d *Datafile) Close() {
	d.reader.Close()
	if d.writer != nil {
		d.CloseWriter()
	}
}

func (d *Datafile) CloseWriter() {
	d.writer.Close()
}
