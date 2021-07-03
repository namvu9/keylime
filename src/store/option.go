package store

// Config represnts the configuration used to initialize the
// store
type Config struct {
	Name    string
	BaseDir string
	T       int
	Storage ReadWriterTo
}

type Option func(*Store)

func WithStorage(rw ReadWriterTo) Option {
	return func(s *Store) {
		s.storage = rw
	}
}

