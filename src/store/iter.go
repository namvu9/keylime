package store

import (
	"fmt"

	"github.com/namvu9/keylime/src/errors"
)

type iterFunc func(*Page) (*Page, error)
type handleFunc func(*Page, *Page)

func (next iterFunc) done(p *Page) bool {
	if p.leaf {
		return true
	}

	nextPage, err := next(p)
	if err != nil {
		return true
	}

	return nextPage == p
}

type collectionIterator struct {
	node    *Page
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

// TODO: Return error
func (ci *collectionIterator) Get() *Page {
	for !ci.next.done(ci.node) {
		nextPage, err := ci.next(ci.node)
		if err != nil {
			fmt.Println(err)
			return nil
		}

		if ci.handler != nil {
			ci.handler(ci.node, nextPage)
		}

		ci.node, _ = ci.next(ci.node)
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
	return func(p *Page) (*Page, error){
		index, exists := p.keyIndex(k)
		
		if exists {
			return p, nil
		}

		return p.Child(index)
	}
}

func byMinPage(p *Page) (*Page, error){
	return p.Child(0)
}

func byMaxPage(p *Page) (*Page, error) {
	return p.Child(len(p.children)-1)
}