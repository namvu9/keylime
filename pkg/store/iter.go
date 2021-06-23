package store

type IterFunc func(*Page) *Page

type CollectionIterator struct {
	key  string
	node *Page
	next IterFunc
}

func (ci *CollectionIterator) forEach(fn func(*Page, *Page) bool) *Page {
	for {
		if ci.node.leaf {
			return ci.node
		}

		next := ci.next(ci.node)
		if next == ci.node {
			return ci.node
		}

		if modified := fn(ci.node, next); !modified {
			ci.node = next
		}
	}
}

func (ci *CollectionIterator) find() *Page {
	return ci.forEach(func(b1, b2 *Page) bool { return false })
}

func (c *BTree) IterBy(next IterFunc) *CollectionIterator {
	return &CollectionIterator{
		next: next,
		node: c.root,
	}
}

func (c *BTree) IterByKey(k string) *CollectionIterator {
	return c.IterBy(func(p *Page) *Page {
		index, exists := p.keyIndex(k)
		if exists {
			return p
		}

		return p.children[index]
	})
}
