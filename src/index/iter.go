package index

import (
	"github.com/namvu9/keylime/src/errors"
)

type iterFunc func(*Node) (*Node, error)
type handleFunc func(*Node, *Node)

func (next iterFunc) done(p *Node) bool {
	if p.Leaf {
		return true
	}

	nextPage, err := next(p)
	if err != nil {
		return true
	}

	return nextPage == p
}

type collectionIterator struct {
	node    *Node
	next    iterFunc
	handler handleFunc
	err     *errors.Error
}

// Err returns the error that caused the iteration to stop
// prematurely. Returns nil if iteration terminated
// successfully.
func (ci *collectionIterator) Err() *errors.Error {
	return ci.err
}

func (ci *collectionIterator) forEach(fn handleFunc) *collectionIterator {
	ci.handler = fn
	return ci
}

func (ci *collectionIterator) Get() (*Node, error) {
	for !ci.next.done(ci.node) {
		nextPage, err := ci.next(ci.node)
		if err != nil {
			return nil, err
		}

		if ci.handler != nil {
			ci.handler(ci.node, nextPage)
		}

		ci.node, _ = ci.next(ci.node)
	}

	return ci.node, nil
}

// maxPage returns an iterator that terminates at the page
// containing the largest key in the tree rooted at `p`.
func (p *Node) maxPage() *collectionIterator {
	return p.iter(byMaxPage)
}

// minPage returns an iterator that terminates at the page
// containing the smallest key in the tree rooted at `p`
func (p *Node) minPage() *collectionIterator {
	return p.iter(byMinPage)
}

// iter returns an iterator that traverses a `Collection`
// of `Pages`, rooted at `p`. The traversal order is
// determined by the `next` callback.
func (p *Node) iter(next iterFunc) *collectionIterator {
	return &collectionIterator{
		next: next,
		node: p,
	}
}

func byKey(k string) iterFunc {
	return func(p *Node) (*Node, error) {
		index, exists := p.keyIndex(k)

		if exists {
			return p, nil
		}

		return p.child(index)
	}
}

func byMinPage(p *Node) (*Node, error) {
	return p.child(0)
}

func byMaxPage(p *Node) (*Node, error) {
	return p.child(len(p.Children) - 1)
}
