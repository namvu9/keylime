package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

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

func main() {
	var (
		tree   = store.New(2, store.WithBasePath("./testdata"), store.WithStorage(FileStorage{root: "./testdata"}))
		reader = bufio.NewReader(os.Stdin)
	)

	// Read config
	// Read origin table

	for {
		fmt.Print("KL> ")

		var (
			text, _ = reader.ReadString('\n')
			tokens  = strings.SplitN(strings.TrimSpace(text), " ", 2)
			cmd     = tokens[0]
		)

		switch strings.ToLower(cmd) {
		case "list":
			fmt.Println("|--------|----------|-----------------------------|")
			fmt.Println("|  Name  |   Root   |         Description         |")
			fmt.Println("|--------|----------|-----------------------------|")
			fmt.Println("| Origin | /origin  | This is the origin database |")
			fmt.Println("|        |          |                             |")
			fmt.Println("|--------|----------|-----------------------------|")
		case "set":
			args := strings.SplitN(tokens[1], " ", 2)
			err := tree.Set(args[0], []byte(args[1]))
			if err != nil {
				fmt.Println(err)
			}
		case "get":
			res := tree.Get(tokens[1])
			fmt.Printf("%s\n", res)
		case "delete":
			err := tree.Delete(tokens[1])
			if err != nil {
				fmt.Println(err)
			}
		case "exit":
			os.Exit(0)
		case "unload":
			tree.Unload()
		}
	}
}
