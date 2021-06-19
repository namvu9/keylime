package store

import (
	"io"
)

type BTree struct {
	root    *BNode
	storage io.ReadWriter
}

func partitionMedian(nums Records) (*Record, Records, Records) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

func (bt *BTree) splitRoot() {
	s := newNode(bt.root.t)
	s.children = []*BNode{bt.root}
	bt.root = s
	s.splitChild(0)
}

func (bt *BTree) splitDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(func(parent, child *BNode, i int) {
		if child.isFull() {
			parent.splitChild(i)
			index, ok := parent.keyIndex(k)
			if ok {
				iter.nextChild = parent
			} else {
				iter.nextChild = parent.children[index]
			}
		}
	}).run()
	return node
}

// TODO: TEST
func handleSparseNode(node, child *BNode, index int) {
	var (
		p = node.childPredecessor(index)
		s = node.childSuccessor(index)
	)

	// Rotate predecessor key
	if p != nil && !p.isSparse() {
		var (
			recordIndex   = index - 1
			pivot         = node.records[recordIndex]
			siblingRecord = p.records.last()
		)

		child.insertRecord(pivot)
		node.setRecord(recordIndex, siblingRecord)

		if !p.leaf {
			// Move child from sibling to child
			siblingLastChild := p.children[len(p.children)-1]
			child.children = append([]*BNode{siblingLastChild}, child.children...)
			p.children = p.children[:len(p.children)-1]
		}
	} else if s != nil && !s.isSparse() {
		var (
			pivot         = node.records[index]
			siblingRecord = s.records[0]
		)

		// Move key from parent to child
		child.records = append(child.records, pivot)
		node.setRecord(index, siblingRecord)

		// Move child from sibling to child
		if !s.leaf {
			siblingFirstChild := s.children[0]
			child.children = append(child.children, siblingFirstChild)
			s.children = s.children[1:]
		}
	} else if p != nil {
		node.mergeChildren(index - 1)
	} else {
		node.mergeChildren(index)
	}
}

// Descend the tree until either the key is found or a leaf
// node is found.
func (bt *BTree) mergeDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(func(parent, child *BNode, i int) {
		if child.isSparse() {
			handleSparseNode(parent, child, i)
			index, ok := parent.keyIndex(k)
			if ok {
				iter.nextChild = parent
			} else {
				iter.nextChild = parent.children[index]
			}
		}
	}).run()
	return node
}

func (bt *BTree) Set(k string, value []byte) error {
	if bt.root.isFull() {
		bt.splitRoot()
	}

	node := bt.splitDescend(k)
	node.insertKey(k, value)

	// Write node
	return nil
}

func (t *BTree) Get(key string) []byte {
	if node, index := t.Search(key); node != nil {
		return node.records[index].value
	}

	return nil
}

type BTreeIterator struct {
	done      bool
	key       string
	node      *BNode
	fn        func(parent, child *BNode, childIndex int)
	nextChild *BNode
}

func (bti *BTreeIterator) forEach(fn func(*BNode, *BNode, int)) *BTreeIterator {
	bti.fn = fn
	return bti
}

func (bti *BTreeIterator) run() (*BNode, bool, int) {
	for {
		index, exists := bti.node.keyIndex(bti.key)
		if exists || bti.node.leaf {
			bti.done = true
			return bti.node, exists, index
		}

		child := bti.node.children[index]
		if bti.fn != nil {
			bti.fn(bti.node, child, index)

		}

		if bti.nextChild != nil {
			bti.node = bti.nextChild
			bti.nextChild = nil
		} else {
			bti.node = child
		}

	}
}

func (t *BTree) iter(key string) *BTreeIterator {
	return &BTreeIterator{
		key:  key,
		node: t.root,
	}
}

func (t *BTree) Search(key string) (*BNode, int) {
	node, ok, index := t.iter(key).run()

	if !ok {
		return nil, index
	}

	return node, index
}

func (b *BTree) Delete(k string) error {
	node := b.mergeDescend(k)
	err := node.deleteKey(k)
	if len(b.root.records) == 0 && len(b.root.children) == 1 {
		b.root = b.root.children[0]
	}

	return err
}

type Option func(*BTree)

// TODO: TEST
func New(t int, opts ...Option) *BTree {
	tree := &BTree{}
	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = newNode(t)
		tree.root.leaf = true
	}

	return tree
}
