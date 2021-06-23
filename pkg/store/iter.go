package store

type IterFunc func(*Page) *Page

type CollectionIterator struct {
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

func (ci *CollectionIterator) Get() *Page {
	return ci.forEach(func(b1, b2 *Page) bool { return false })
}

func (c *Page) IterBy(next IterFunc) *CollectionIterator {
	return &CollectionIterator{
		next: next,
		node: c,
	}
}

func (c *Page) IterByKey(k string) *CollectionIterator {
	return c.IterBy(func(p *Page) *Page {
		index, exists := p.keyIndex(k)
		if exists {
			return p
		}

		return p.children[index]
	})
}

func (c *Page) MaxPage() *CollectionIterator {
	return c.IterBy(func(p *Page) *Page {
		return p.children[len(p.children)-1]
	})
}

func (c *Page) MinPage() *CollectionIterator {
	return c.IterBy(func(p *Page) *Page {
		return p.children[0]
	})
}
