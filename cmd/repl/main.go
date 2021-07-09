package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/namvu9/keylime/src/keylime"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	client, err := keylime.Connect("localhost", "1337")
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	for {
		fmt.Print("KL> ")

		input, err := reader.ReadString(';')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println()
				return
			}
			fmt.Println(err)
			continue
		}

		client.WriteString(input)
	}
}
