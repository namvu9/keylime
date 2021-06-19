all: one two three

cli:
	go run ./cmd/cli/main.go
lint:
	go vet ./...
test_watch: 
	gow test ./...
coverage:
	go test -coverprofile coverage.out ./...
	go tool cover -html=coverage.out
clean:
	rm coverage.out
