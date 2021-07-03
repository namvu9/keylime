package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/namvu9/keylime/src/queries"
	"github.com/namvu9/keylime/src/store"
)

type FStorage struct {
	location string
	offset   int64
}

func (fs *FStorage) Delete() error {
	return nil
}

func (fs *FStorage) Read(dst []byte) (int, error) {
	f, err := os.Open(fs.location)
	defer f.Close()
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(dst, int64(fs.offset))
	fs.offset += int64(n)

	if err == io.EOF {
		fs.offset = 0
	}

	return n, err
}

func (fs *FStorage) Write(src []byte) (int, error) {
	if src == nil {
		return 0, os.MkdirAll(fs.location, 0755)
	}

	return 0, ioutil.WriteFile(fs.location, src, 0755)
}

func (fs *FStorage) Exists() (bool, error) {
	if _, err := os.Stat(fs.location); os.IsNotExist(err) {
		return false, nil
	} else {
		return true, err
	}
}

func (fs *FStorage) WithSegment(name string) store.ReadWriterTo {
	return &FStorage{
		location: path.Join(fs.location, name),
	}
}

var fs = &FStorage{"./testdata", 0}
var (
	cfg = &store.Config{
		T:       200,
		BaseDir: "./testdata",
	}

	s      = store.New(cfg, store.WithStorage(fs))
	reader = bufio.NewReader(os.Stdin)
)

func main() {
	for {
		fmt.Print("KL> ")

		var (
			ctx     = context.Background()
			text, _ = reader.ReadString(';')
		)

		op, err := queries.Parse(text)
		if err != nil {
			fmt.Println(err)
			continue
		}

		res, err := s.Run(ctx, *op)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if res != nil {
			fmt.Println(res)
		}
	}
}
