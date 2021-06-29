package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

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
		return false, err
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
		ctx := context.Background()

		var (
			text, _ = reader.ReadString('\n')
			tokens  = strings.Split(strings.TrimSpace(text), " ")
		)

		if len(tokens) < 1 {
			fmt.Println("Syntax error: At least one command must be specified")
			continue
		}

		if err := handleCmd(ctx, tokens[0], tokens[1:]); err != nil {
			fmt.Println(err)
		}
	}
}

func handleCmd(ctx context.Context, cmd string, args []string) error {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	c, err := s.Collection("test")
	if err != nil {
		return err
	}

	switch strings.ToLower(cmd) {
	case "set":
		if len(args) < 2 {
			return fmt.Errorf("Syntax Error: Set requires exactly 2 arguments")
		}

		key := args[0]
		value := strings.Join(args[1:], " ")
		return c.Set(ctx, key, []byte(value))

	case "get":
		if len(args) < 1 {
			return fmt.Errorf("Syntax Error: Set requires exactly 1 argument")
		}
		res, err := c.Get(ctx, args[0])
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", res)
	case "delete":
		if len(args) < 1 {
			return fmt.Errorf("Syntax Error: Set requires exactly 1 argument")
		}
		return c.Delete(ctx, args[0])
	case "exit":
		os.Exit(0)
	}

	return nil
}
