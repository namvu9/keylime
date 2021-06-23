package store

type Option func(*BTree)

type NodeReadWriter interface {

}

func WithStorage(s NodeReadWriter) Option {
	return func(b *BTree) {
		b.storage = s
	}
}

func WithBasePath(path string) Option {
	return func(b *BTree) {
		b.basePath = path
	}
}

func WithRoot(root *Page) Option {
	return func(b *BTree) {
		b.root = root
	}
}
