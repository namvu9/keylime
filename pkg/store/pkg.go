/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

// New returns a store
func New(t int, opts ...Option) *Collection {
	tree := &Collection{
		t: t,
	}

	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = tree.newPage()
		tree.root.leaf = true
	}

	return tree
}
