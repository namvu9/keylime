package store

import (
	"fmt"

	"github.com/namvu9/keylime/pkg/record"
)

type BTree struct {
	T        int
	root     *BNode
	storage  NodeReadWriter
	basePath string
	cr       ChangeReporter
}

func partitionMedian(nums []record.Record) (record.Record, []record.Record, []record.Record) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

func (bt *BTree) splitRoot() {
	s := bt.newNode()
	s.children = []*BNode{bt.root}
	bt.root = s
	s.splitChild(0)

	s.registerWrite("Split root")
}

// TODO: TEST
func handleSparseNode(node, child *BNode, index int) bool {
	if !child.isSparse() {
		return false
	}

	var (
		p = node.childPredecessor(index)
		s = node.childSuccessor(index)
	)

	child.registerWrite("Sparse node")
	node.registerWrite("Sparse node (parent)")

	// Rotate predecessor key
	if p != nil && !p.isSparse() {
		p.registerWrite("Sparse node (predecessor)")
		var (
			recordIndex   = index - 1
			pivot         = node.records[recordIndex]
			siblingRecord = p.records[len(p.records)-1]
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
		s.registerWrite("Sparse node (successor)")
		var (
			pivot         = node.records[index]
			siblingRecord = s.records[0]
		)

		// Move key from parent to child
		child.records = append(child.records, pivot)
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

	return true
}

func handleFullNode(node, child *BNode, index int) bool {
	if !child.isFull() {
		return false
	}

	node.splitChild(index)

	return true
}

func (bt *BTree) mergeDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(handleSparseNode)
	return node
}

func (bt *BTree) splitDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(handleFullNode)
	return node
}

func (t *BTree) iter(key string) *BTreeIterator {
	return &BTreeIterator{
		key:  key,
		node: t.root,
	}
}

func (b *BTree) newNode() *BNode {
	node := newNode(b.T)
	node.storage = &b.cr
	return node
}

func (b *BTree) Delete(k string) error {
	node := b.mergeDescend(k)
	err := node.deleteKey(k)
	if len(b.root.records) == 0 && len(b.root.children) == 1 {
		b.root = b.root.children[0]
	}

	if err != nil {
		return err
	}

	return nil
}

func (bt *BTree) Set(k string, value []byte) error {
	if bt.root.isFull() {
		bt.splitRoot()
	}

	node := bt.splitDescend(k)
	fmt.Println(node)
	node.insertKey(k, value)

	// Commit writes
	return nil
}

func (t *BTree) Get(key string) []byte {
	if node, ok, index := t.iter(key).find(); ok {
		return node.records[index].Value()
	} else {
		return nil
	}
}

// TODO: TEST
func New(t int, opts ...Option) *BTree {
	tree := &BTree{
		T:  t,
		cr: ChangeReporter{},
	}

	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = tree.newNode()
		tree.root.Leaf = true
	}

	return tree
}
