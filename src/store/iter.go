package store

import "fmt"

type iterFunc func(*page) *page
type handleFunc func(*page, *page)

func (next iterFunc) done(p *page) bool {
	if !p.loaded {
		// TODO: handle error
		err := p.load()
		if err != nil {
			fmt.Println("ERROR: could not load page", err)
			return true
		}
	}

	if p.leaf {
		return true
	}

	return next(p) == p
}

type collectionIterator struct {
	node    *page
	next    iterFunc
	handler handleFunc
	err     error
}

func (ci *collectionIterator) forEach(fn handleFunc) *collectionIterator {
	ci.handler = fn
	return ci
}

func (ci *collectionIterator) Get() *page {
	for !ci.next.done(ci.node) {
		if ci.handler != nil {
			ci.handler(ci.node, ci.next(ci.node))
		}

		ci.node = ci.next(ci.node)
	}

	return ci.node
}

// maxPage returns an iterator that terminates at the page
// containing the largest key in the tree rooted at `p`.
func (p *page) maxPage() *collectionIterator {
	return p.iter(byMaxPage)
}

// minPage returns an iterator that terminates at the page
// containing the smallest key in the tree rooted at `p`
func (p *page) minPage() *collectionIterator {
	return p.iter(byMinPage)
}

// iter returns an iterator that traverses a `Collection`
// of `Pages`, rooted at `p`. The traversal order is
// determined by the `next` callback.
func (p *page) iter(next iterFunc) *collectionIterator {
	return &collectionIterator{
		next: next,
		node: p,
	}
}

func byKey(k string) iterFunc {
	return func(p *page) *page {
		index, exists := p.keyIndex(k)
		if exists {
			return p
		}

		return p.children[index]
	}
}

func byMinPage(p *page) *page {
	return p.children[0]
}

func byMaxPage(p *page) *page {
	return p.children[len(p.children)-1]
}
