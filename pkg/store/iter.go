package store

type IterFunc func(*Page) *Page
type HandleFunc func(*Page, *Page)

type CollectionIterator struct {
	node     *Page
	next     IterFunc
	handlers []HandleFunc
}

func (ci *CollectionIterator) forEach(fn func(*Page, *Page)) *CollectionIterator {
	ci.handlers = append(ci.handlers, fn)
	return ci
}

func (ci *CollectionIterator) Get() *Page {
	for {
		if ci.node.leaf {
			return ci.node
		}

		next := ci.next(ci.node)
		if next == ci.node {
			return ci.node
		}

		for _, handle := range ci.handlers {
			handle(ci.node, next)
		}

		next = ci.next(ci.node)
		if next == ci.node {
			return ci.node
		}

		ci.node = next
	}
}

// Max returns an iterator that terminates at the page
// containing the largest key in the tree rooted at `p`.
func (p *Page) Max() *CollectionIterator {
	return p.iter(byMaxPage)
}

// MinPage returns an iterator that terminates at the page
// containing the smallest key in the tree rooted at `p`
func (p *Page) MinPage() *CollectionIterator {
	return p.iter(byMinPage)
}

// iter returns an iterator that traverses a `Collection`
// of `Pages`, rooted at `p`. The traversal order is
// determined by the `next` callback.
func (p *Page) iter(next IterFunc) *CollectionIterator {
	return &CollectionIterator{
		next: next,
		node: p,
	}
}

func byKey(k string) IterFunc {
	return func(p *Page) *Page {
		index, exists := p.keyIndex(k)
		if exists {
			return p
		}

		return p.children[index]
	}
}

func byMinPage(p *Page) *Page {
	return p.children[0]
}

func byMaxPage(p *Page) *Page {
	return p.children[len(p.children)-1]
}
