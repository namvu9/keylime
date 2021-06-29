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
	c      *store.Collection
	reader = bufio.NewReader(os.Stdin)
)

func main() {
	for {
		if c == nil {
			fmt.Print("KL> ")
		} else {
			fmt.Printf("[%s]> ", c.Name)
		}
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

	switch strings.ToLower(cmd) {
	case "set":
		if len(args) < 2 {
			return fmt.Errorf("Syntax Error: Set requires exactly 2 arguments")
		}

		key := args[0]
		value := strings.Join(args[1:], " ")
		err := c.Set(ctx, key, []byte(value))
		if err != nil {
			return err
		}

		fmt.Println("Successfully saved record with key", key)

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
	case "collection":
		if len(args) != 1 {
			return fmt.Errorf("Syntax Error: Set requires exactly 1 argument")
		}

		collection, err := s.Collection(args[0])
		if err != nil {
			return err
		}

		fmt.Println("Successfully created/loaded collection", args[0])
		c = collection

	case "exit":
		os.Exit(0)

	case "info":
		if c == nil {
			s.Info()
		} else {
			c.Info()
		}
	case "info-all":
		s.Info()
	default:
		fmt.Println("Unknown command:", cmd)
	}

	return nil
}
