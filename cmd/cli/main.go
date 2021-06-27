package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/namvu9/keylime/src/store"
)

var (
	cfg = &store.Config{
		T:       200,
		BaseDir: "./testdata",
	}

	s      = store.New(cfg)
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
	case "list":
		for _, collection := range s.Collections() {
			fmt.Printf("\n-----------\nCollections\n-----------\n")
			fmt.Println(collection.Name)
			fmt.Printf("\n-----------\n")
		}
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
