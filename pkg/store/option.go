package store

type Option func(*Collection)

type NodeReadWriter interface {

}

func WithStorage(s NodeReadWriter) Option {
	return func(b *Collection) {
		b.storage = s
	}
}

func WithBasePath(path string) Option {
	return func(b *Collection) {
		b.basePath = path
	}
}

func WithRoot(root *Page) Option {
	return func(b *Collection) {
		b.root = root
	}
}
