package store

// Config represnts the configuration used to initialize the
// store
type Config struct {
	BaseDir string
	Port    string
	Host    string
}

type Option func(*Store)
