package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/namvu9/keylime/src/fs"
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
		s      = store.New(cfg, store.WithStorage(fs.New(cfg.BaseDir)))
		reader = bufio.NewReader(os.Stdin)
	)

	timeout := time.Minute

	script := flag.String("script", "", "Location of script to run")
	flag.Parse()
	if *script != "" {
		data, err := ioutil.ReadFile(*script)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		statements := strings.Split(string(data), ";")
		for _, input := range statements {
			fmt.Println("Running", input)
			res, err := queries.Interpret(context.Background(), s, input)
			if err != nil {
				fmt.Println(err)
			}

			if res != nil {
				fmt.Println(res)
			}
		}
	}

	for {
		fmt.Print("KL> ")

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
