package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/namvu9/keylime/pkg/store"
)

func main() {
	var (
		tree   = store.New(2, store.WithBasePath("./testdata"))
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
		}
	}
}
