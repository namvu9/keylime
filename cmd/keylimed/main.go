package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/namvu9/keylime/src/queries"
	"github.com/namvu9/keylime/src/store"
)

func readConfig() (*store.Config, error) {
	cfg := &store.Config{
		T:       200,
		BaseDir: "./testdata",
	}

	return cfg, nil
}

func main() {
	cfg, err := readConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not load config: %s", err)
		os.Exit(1)
	}

	var (
		//s      = store.New(cfg, store.WithStorage(repository.NewFS(cfg.BaseDir)))
		s      = store.New(cfg)
		reader = bufio.NewReader(os.Stdin)
	)

	timeout := time.Minute

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		input, err := reader.ReadString(';')
		if err != nil {
			fmt.Println(err)
			continue
		}

		done := make(chan interface{})
		go func() {
			res, err := queries.Interpret(ctx, s, input)
			if err != nil {
				fmt.Println(err)
				cancel()
				return
			}

			done <- res
		}()

		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				fmt.Println("Error: request timed out")
			}

		case v := <-done:
			if v != nil {
				fmt.Println(v)
			}
		}
	}
}
