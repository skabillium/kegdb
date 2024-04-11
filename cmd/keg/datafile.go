package keg

import "os"

type Datafile struct {
	reader *os.File
	writer *os.File

	id int
}

// func NewDatafile(id int, write bool) (*Datafile, error) {
// 	reader, err := os.Open("")
// }
