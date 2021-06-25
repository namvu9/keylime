package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/namvu9/keylime/pkg/store"
)

func main() {
	var (
		cfg = &store.Config{
			T:       2,
			BaseDir: "./testdata",
		}

		s      = store.New(cfg)
		reader = bufio.NewReader(os.Stdin)
	)

	err := s.Init()
	if err != nil {
		log.Fatal(err)
	}

	c, err := s.Collection("test")
	if err != nil {
		log.Fatal(err)
	}

	for {
		fmt.Print("KL> ")

		var (
			text, _ = reader.ReadString('\n')
			tokens  = strings.SplitN(strings.TrimSpace(text), " ", 2)
			cmd     = tokens[0]
		)

		switch strings.ToLower(cmd) {
		case "list":
			for _, collection := range s.Collections() {
				fmt.Printf("\n-----------\nCollections\n-----------\n")
				fmt.Println(collection.Name)
				fmt.Printf("\n-----------\n")
			}
		case "set":
			args := strings.SplitN(tokens[1], " ", 2)
			err := c.Set(args[0], []byte(args[1]))
			if err != nil {
				fmt.Println(err)
			}
		case "get":
			res := c.Get(tokens[1])
			fmt.Printf("%s\n", res)
		case "delete":
			err := c.Delete(tokens[1])
			if err != nil {
				fmt.Println(err)
			}
		case "exit":
			os.Exit(0)
		}
	}
}
