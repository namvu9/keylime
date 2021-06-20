package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/namvu9/keylime/pkg/store"
)

type FileStorage struct {
	root string
}

func (fs FileStorage) Read(location string, b *store.BNode) error {
	if location == "" {
		return fmt.Errorf("Cannot read from nil location")
	}

	fmt.Printf("Reading from %s\n", path.Join(fs.root, location))

	data, err := os.ReadFile(path.Join(fs.root, location))
	if err != nil {
		return err
	}

	var (
		buffer = bytes.NewBuffer(data)
		dec    = gob.NewDecoder(buffer)
	)
	if err := dec.Decode(b); err != nil {
		return err
	}

	return nil
}
func (f FileStorage) Write(location string, data []byte) (int, error) {
	err := os.WriteFile(path.Join(f.root, location), data, fs.FileMode(0655))
	if err != nil {
		return 0, err
	}
	fmt.Printf("Wrote to %s\n", location)

	return 0, nil
}
