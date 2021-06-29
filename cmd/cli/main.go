package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
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

// TODO: IMPLEMENT
func (fs *FStorage) Read(dst []byte) (int, error) {
	f, err := os.Open(fs.location)
	defer f.Close()
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(dst, int64(fs.offset))
	fs.offset += int64(n)

	return n, err
}

func (fs *FStorage) Write(src []byte) (int, error) {
	if src == nil {
		return 0, os.MkdirAll(fs.location, 0755)
	}

	fmt.Println("WRITE", fs.location)
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
			tokens  = strings.SplitN(strings.TrimSpace(text), " ", 2)
			cmd     = tokens[0]
		)

		if err := handleCmd(ctx, cmd, tokens); err != nil {
			fmt.Println(err)
		}
	}
}

func handleCmd(ctx context.Context, cmd string, tokens []string) error {
	c, err := s.Collection("test")
	if err != nil {
		log.Fatal(err)
	}

	switch strings.ToLower(cmd) {
	case "set":
		args := strings.SplitN(tokens[1], " ", 2)
		return c.Set(ctx, args[0], []byte(args[1]))
	case "set-if":
		args := strings.SplitN(tokens[1], " ", 2)
		if c.Get(ctx, args[0]) == nil {
			return c.Set(ctx, args[0], []byte(args[1]))
		}
	case "get":
		res := c.Get(ctx, tokens[1])
		if res == nil {
			return fmt.Errorf("KeyNotFound")
		}
		fmt.Printf("%s\n", res)
	case "delete":
		return c.Delete(ctx, tokens[1])
	case "exit":
		os.Exit(0)
	}

	return nil
}
