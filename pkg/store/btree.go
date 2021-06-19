package store

import (
	"fmt"
	"os"
	"path"
)

type BTree struct {
	T        int
	root     *BNode
	storage  NodeReadWriter
	basePath string
}

func (b *BTree) Unload() {
	b.root = nil
	loadTree(b)
}

func partitionMedian(nums Records) (*Record, Records, Records) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

func (bt *BTree) splitRoot() {
	s := bt.newNode()

	// TODO: HACK
	bt.root.ID = s.ID
	bt.root.write()
	s.ID = "origin"

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
			pivot         = node.Records[recordIndex]
			siblingRecord = p.Records.last()
		)

		child.insertRecord(pivot)
		node.setRecord(recordIndex, siblingRecord)

		if !p.Leaf {
			// Move child from sibling to child
			siblingLastChild := p.children[len(p.children)-1]
			child.children = append([]*BNode{siblingLastChild}, child.children...)
			p.children = p.children[:len(p.children)-1]
		}
	} else if s != nil && !s.isSparse() {
		var (
			pivot         = node.Records[index]
			siblingRecord = s.Records[0]
		)

		// Move key from parent to child
		child.Records = append(child.Records, pivot)
		node.setRecord(index, siblingRecord)

		// Move child from sibling to child
		if !s.Leaf {
			siblingFirstChild := s.children[0]
			child.children = append(child.children, siblingFirstChild)
			s.children = s.children[1:]
		}
	} else if p != nil {
		node.mergeChildren(index - 1)
	} else {
		node.mergeChildren(index)
	}
	// Write nodes
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

	fmt.Printf("Write after inserting key %s into %v\n", k, node.ID)
	_, err := node.write()
	if err != nil {
		return err
	}

	return nil
}

func (t *BTree) Get(key string) []byte {
	if node, index := t.Search(key); node != nil {
		return node.Records[index].Value
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
		if exists || bti.node.Leaf {
			bti.done = true
			return bti.node, exists, index
		}

		child := bti.node.children[index]
		if err := child.read(); err != nil {
			panic(err)
		}

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
	if len(b.root.Records) == 0 && len(b.root.children) == 1 {
		b.root = b.root.children[0]
	}

	return err
}

type Option func(*BTree)

func WithStorage(s NodeReadWriter) Option {
	return func(b *BTree) {
		b.storage = s
	}
}

func WithBasePath(path string) Option {
	return func(b *BTree) {
		b.basePath = path
	}
}

func (b *BTree) newNode() *BNode {
	node := newNode(b.T)
	node.storage = b.storage
	return node
}

func loadTree(tree *BTree) error {
	root := tree.newNode()
	root.ID = "origin"
	tree.root = root
	tree.root.storage = tree.storage

	if _, err := os.Stat(path.Join(tree.basePath, "origin")); os.IsNotExist(err) {
		tree.root.Leaf = true
		tree.root.loaded = true
		tree.root.write()
	} else {
		err := tree.root.read()
		if err != nil {
			return err
		}
		tree.T = tree.root.T
	}
	return nil
}

// TODO: TEST
func New(t int, opts ...Option) *BTree {
	tree := &BTree{T: t}
	for _, fn := range opts {
		fn(tree)
	}

	err := loadTree(tree)
	if err != nil {
		fmt.Println(err)
	}

	return tree
}
