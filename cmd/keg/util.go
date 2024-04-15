package keg

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Get current timestamp as a uint32 for use in headers
func now() uint32 {
	return uint32(time.Now().Unix())
}

// Check if a file/directory exists
func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

// List files containing the database data
func (k *Keg) listDataFiles() ([]string, error) {
	regex, err := regexp.Compile(`keg-(\d+).db`)
	if err != nil {
		return nil, err
	}

	files := []string{}
	err = filepath.Walk(k.dataDir, func(path string, info fs.FileInfo, err error) error {
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

// Get the id for the new active file
func (k *Keg) getNextFileId() int {
	ids := []int{}

	files, err := k.listDataFiles()
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

// Extract id from data file's name
func getFileIdFromName(name string) int {
	split := strings.Split(name, ".")
	id, err := strconv.Atoi(split[0][len("keg-"):])
	if err != nil {
		return -1
	}
	return id
}
