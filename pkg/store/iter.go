package store

type IterFunc func(*Page) *Page
type HandleFunc func(*Page, *Page) bool

type CollectionIterator struct {
	node     *Page
	next     IterFunc
	handlers []HandleFunc
}

func (ci *CollectionIterator) ForEach(fn func(*Page, *Page) bool) *CollectionIterator {
	ci.handlers = append(ci.handlers, fn)
	return ci
}

func (ci *CollectionIterator) Get() *Page {
	for {
		var modified bool

		if ci.node.leaf {
			return ci.node
		}

		next := ci.next(ci.node)
		if next == ci.node {
			return ci.node
		}

		for _, handle := range ci.handlers {
			if res := handle(ci.node, next); res {
				modified = true
			}
		}

		if !modified {
			ci.node = next
		}
	}
}

// Iter returns an iterator that traverses a `Collection`
// of `Pages`, rooted at `p`. The traversal order is
// determined by the `next` callback.
func (p *Page) Iter(next IterFunc) *CollectionIterator {
	return &CollectionIterator{
		next: next,
		node: p,
	}
}

func ByKey(k string) IterFunc {
	return func(p *Page) *Page {
		index, exists := p.keyIndex(k)
		if exists {
			return p
		}

		return p.children[index]
	}
}

func ByMinPage(p *Page) *Page {
	return p.children[0]
}

func ByMaxPage(p *Page) *Page {
	return p.children[len(p.children)-1]
}

// Max returns an iterator that terminates at the page
// containing the largest key in the tree rooted at `p`.
func (p *Page) Max() *CollectionIterator {
	return p.Iter(ByMaxPage)
}

// MinPage returns an iterator that terminates at the page
// containing the smallest key in the tree rooted at `p`
func (p *Page) MinPage() *CollectionIterator {
	return p.Iter(ByMinPage)
}
