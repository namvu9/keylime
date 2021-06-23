package store

type Option func(*Collection)

func WithRoot(root *Page) Option {
	return func(b *Collection) {
		b.root = root
	}
}
