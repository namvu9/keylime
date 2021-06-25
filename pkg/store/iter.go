package store

import "fmt"

// collection iterator.
type iterFunc func(*Page) *Page
type handleFunc func(*Page, *Page)

func (next iterFunc) done(p *Page) bool {
	if p.leaf {
		return true
	}

	return next(p) == p
}

type collectionIterator struct {
	node    *Page
	next    iterFunc
	handler handleFunc
	err     error
}

func (ci *collectionIterator) forEach(fn func(*Page, *Page)) *collectionIterator {
	ci.handler = fn
	return ci
}

func (ci *collectionIterator) Get() *Page {
	for !ci.next.done(ci.node) {
		if ci.handler != nil {
			ci.handler(ci.node, ci.next(ci.node))
		}

		ci.node = ci.next(ci.node)

		if !ci.node.loaded {
			// TODO: Return error
			err := ci.node.load()
			if err != nil {
				fmt.Println("ERROR:", err)
			}
		}
	}

	return ci.node
}

// maxPage returns an iterator that terminates at the page
// containing the largest key in the tree rooted at `p`.
func (p *Page) maxPage() *collectionIterator {
	return p.iter(byMaxPage)
}

// minPage returns an iterator that terminates at the page
// containing the smallest key in the tree rooted at `p`
func (p *Page) minPage() *collectionIterator {
	return p.iter(byMinPage)
}

// iter returns an iterator that traverses a `Collection`
// of `Pages`, rooted at `p`. The traversal order is
// determined by the `next` callback.
func (p *Page) iter(next iterFunc) *collectionIterator {
	return &collectionIterator{
		next: next,
		node: p,
	}
}

func byKey(k string) iterFunc {
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
