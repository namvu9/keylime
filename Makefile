all: one two three

repl:
	KEYLIME_HOME=/home/nam/go/src/github.com/namvu9/keylime/testdata \
	go run ./cmd/repl
lint:
	go vet ./...
test_watch: 
	gow test ./...
coverage:
	go test -coverprofile coverage.out ./...
	go tool cover -html=coverage.out
clean:
	rm coverage.out
