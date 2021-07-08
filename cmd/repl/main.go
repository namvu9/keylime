package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	timeout := time.Minute

	for {
		fmt.Print("KL> ")

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		input, err := reader.ReadString(';')
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println(input, ctx, cancel)
	}
}
