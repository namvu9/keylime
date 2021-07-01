package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/namvu9/keylime/src/store"
	"github.com/namvu9/keylime/src/types"
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
	collection, err := s.Collection("users")
	if err != nil {
		fmt.Println(err)
	}

	if collection.Exists() {
		err := collection.Load()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Successfully loaded collection", "users")
		c = collection
	}

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
			return fmt.Errorf("syntax error: Set <key> key1=value1 key2=value2 ...")
		}

		key := args[0]
		fields := make(map[string]interface{})

		for _, kv := range args[1:] {
			data := strings.Split(kv, "=")
			if len(data) != 2 {
				return fmt.Errorf("Syntax error: Set <key> key1=value1 key2=value2 ...")
			}

			fields[data[0]] = data[1]
		}
		err := c.Set(ctx, key, fields)
		if err != nil {
			return err
		}

		fmt.Println("Successfully saved record with key", key)
	case "update":
		if len(args) < 2 {
			return fmt.Errorf("Syntax Error: Set requires exactly 2 arguments")
		}

		key := args[0]
		fields := make(map[string]interface{})

		for _, kv := range args[1:] {
			data := strings.Split(kv, "=")
			if len(data) != 2 {
				return fmt.Errorf("Syntax error: Set <key> key1=value1 key2=value2 ...")
			}

			fields[data[0]] = data[1]
		}

		err := c.Update(ctx, key, fields)
		if err != nil {
			return err
		}

	case "get":
		if len(args) < 1 {
			return fmt.Errorf("Syntax Error: Get requires at least 1 argument")
		}

		key := args[0]

		rec, err := c.Get(ctx, key)
		if err != nil {
			return err
		}

		if len(args) > 1 {
			selectors := types.MakeFieldSelectors(args[1:]...)
			res := rec.Select(selectors...)
			s, _ := types.Prettify(res)
			fmt.Printf("%s=%s\n", key, s)
		} else {
			fmt.Println(rec)
		}

		return nil

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
		if collection.Exists() {
			collection.Load()
			if err != nil {
				return err
			}
			fmt.Println("Successfully loaded collection", args[0])
		} else {
			var schema *types.Schema
			if args[0] == "users" {
				sb := types.NewSchemaBuilder()
				sb.AddField("name", types.String)
				sb.AddField("email", types.String, types.Optional, types.WithDefault("dufus@gmail.com"))
				sb.AddField("age", types.Number, types.WithDefault(4))
				personSchema, _ := sb.Build()

				sb = types.ExtendSchema(personSchema)
				sb.AddField("people", types.Array, types.WithElementType(types.Number), types.WithMin(1))
				sb.AddField("Object", types.Object, types.WithSchema(personSchema))

				userSchema, errs := sb.Build()
				if errs != nil {
					fmt.Println(errs)
					return err
				}

				schema = userSchema
			}

			err = collection.Create(schema)
			if err != nil {
				return err
			}

			if args[0] == "users" {
				fields := map[string]interface{}{
					"name":   "Nam",
					"age":    10,
					"people": []interface{}{4},
					"Object": map[string]interface{}{
						"name":  "BITCH",
						"age":   99,
						"email": "9319vuna@gmail.com",
					},
				}

				for i := 0; i < 4; i++ {
					err = collection.Set(ctx, fmt.Sprint(i), fields)
					if err != nil {
						return err
					}
				}
			}

			fmt.Println("Successfully created collection", args[0])
		}

		c = collection

	case "exit":
		os.Exit(0)

	case "info":
		if c == nil {
			s.Info()
		} else {
			c.Info()
		}

	case "head":
		if len(args) != 1 {
			return fmt.Errorf("Syntax Error: head requires exactly 1 argument")
		}

		n, _ := strconv.ParseInt(args[0], 0, 0)

		res := c.GetFirst(ctx, int(n))
		fmt.Println(len(res), res)

		return nil
	case "tail":
		if len(args) != 1 {
			return fmt.Errorf("Syntax Error: tail requires exactly 1 argument")
		}

		n, _ := strconv.ParseInt(args[0], 0, 0)

		res := c.GetLast(ctx, int(n))
		fmt.Println(len(res), res)

		return nil

	case "info-all":
		s.Info()
	default:
		fmt.Println("Unknown command:", cmd)
	}

	return nil
}
