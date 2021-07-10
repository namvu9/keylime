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

	nextNode, err := next(p)
	if err != nil {
		return true
	}

	return nextNode == p
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
		nextNode, err := ci.next(ci.node)
		if err != nil {
			return nil, err
		}

		if ci.handler != nil {
			ci.handler(ci.node, nextNode)
		}

		ci.node, _ = ci.next(ci.node)
	}

	return ci.node, nil
}

// maxNode returns an iterator that terminates at the node
// containing the largest key in the tree rooted at `p`.
func (p *Node) maxNode() *collectionIterator {
	return p.iter(byMaxNode)
}

// minNode returns an iterator that terminates at the node
// containing the smallest key in the tree rooted at `p`
func (p *Node) minNode() *collectionIterator {
	return p.iter(byMinNode)
}

// iter returns an iterator that traverses a `Collection`
// of `Nodes`, rooted at `p`. The traversal order is
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

func byMinNode(p *Node) (*Node, error) {
	return p.child(0)
}

func byMaxNode(p *Node) (*Node, error) {
	return p.child(len(p.Children) - 1)
}
