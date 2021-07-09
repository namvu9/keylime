package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/namvu9/keylime/src/queries"
	"github.com/namvu9/keylime/src/store"
)

func prettify(v interface{}) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

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

	listener, err := net.Listen("tcp", "127.0.0.1:1337")
	if err != nil {
		log.Fatal(err)
	}

	s := store.New(cfg)
	timeout := time.Minute

	log.Printf("Listening on %s\n", listener.Addr())

	for {
		conn, _ := listener.Accept()

		go func(c net.Conn) {
			defer c.Close()
			log.Printf("Accepted incoming connection from %s\n", c.RemoteAddr())

			for {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				buf := make([]byte, 1000)
				n, err := conn.Read(buf)
				if err != nil {
					if errors.Is(err, io.EOF) {
						log.Printf("Connection closed by %s\n", c.RemoteAddr())
					} else {
						log.Println(err)
					}
					return
				}

				res, err := queries.Interpret(ctx, s, string(buf[:n]))
				if err != nil {
					log.Printf("Error: %s\n", err)
					conn.Write([]byte(fmt.Sprintf("Error: %s", err)))
				} else if res != nil {
					s, _ := prettify(res)
					conn.Write([]byte(s))
				} else {
					conn.Write([]byte("OK"))
				}
			}
		}(conn)
	}
}
