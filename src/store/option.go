package store

// Config represnts the configuration used to initialize the
// store
type Config struct {
	Name    string
	BaseDir string
	T       int
	Storage ReadWriterTo
	Port    int
}

type Option func(*Store)
