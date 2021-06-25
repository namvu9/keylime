package store

// TODO: REMOVE
type CollectionOption func(*Collection)

func WithRoot(root *Page) CollectionOption {
	return func(b *Collection) {
		b.root = root
	}
}

type Option interface {
	Apply(*Store)
}

// Config represnts the configuration used to initialize the
// store
type Config struct {
	Name    string `json:"name"`
	BaseDir string `json:"baseDir"`
	T       int    `json:"minDegree"`
}
