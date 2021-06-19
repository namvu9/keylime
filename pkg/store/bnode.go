package store

import (
	"context"
	"fmt"
	"strings"
)

type BNode struct {
	children []*BNode
	records  Records
	leaf     bool
	t        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).
}

func newNode(t int) *BNode {
	return &BNode{
		children: []*BNode{},
		records:  make([]*Record, 0, 2*t-1),
		leaf:     false,
		t:        t,
	}
}

func (b *BNode) isFull() bool {
	return len(b.records) == 2*b.t-1
}

type nodeReference struct {
	*BNode
	location string
}

func (nr *nodeReference) load(ctx context.Context) error {
	// With cancel?
	if loader := ctx.Value("Loader"); loader != nil {

	}
	return nil
}

// keyIndex returns the index of key k in node b if it
// exists. Otherwise, it returns the index of the subtree
// where the key could be possibly be found
func (b *BNode) keyIndex(k string) (index int, exists bool) {
	for i, kv := range b.records {
		if k == kv.key {
			return i, true
		}

		if strings.Compare(k, kv.key) < 0 {
			return i, false
		}
	}

	return len(b.records), false
}

// insert key `k` into node `b` in sorted order. Panics if node is full. Returns the index at which the key was inserted
// TODO: TEST RETURN INDEX
// TODO: TEST
func (b *BNode) insert(k string, value []byte) int {
	if b.isFull() {
		panic("Cannot insert key into full node")
	}

	kv := NewRecord(k, value)
	out := []*Record{}

	for i, key := range b.records {
		if kv.key == key.key {
			b.records[i] = kv
			return i
		}

		if kv.isLessThan(key) {
			out = append(out, kv)
			b.records = append(out, b.records[i:]...)
			return i
		} else {
			out = append(out, b.records[i])
		}
	}

	b.records = append(out, kv)
	return len(b.records) - 1
}

// Panics if child is not full
func (b *BNode) splitChild(index int) {
	fullChild := b.children[index]
	if !fullChild.isFull() {
		panic("Cannot split non-full child")
	}

	newChild := newNode(b.t)
	newChild.leaf = fullChild.leaf

	medianKey, left, right := partitionMedian(fullChild.records)
	b.insert(medianKey.key, medianKey.value)

	fullChild.records, newChild.records = left, right

	if !fullChild.leaf {
		newChild.insertChildren(0, fullChild.children[b.t:]...)
		fullChild.children = fullChild.children[:b.t]
	}

	b.insertChildren(index+1, newChild)
}

func (b *BNode) insertRecord(r *Record) int {
	return b.insert(r.key, r.value)
}

func (b *BNode) setRecord(index int, r *Record) {
	b.records[index] = r
}

func (b *BNode) insertChildren(index int, children ...*BNode) {
	if len(b.children) == 2*b.t {
		panic("Cannot insert a child into a full node")
	}

	if index > len(b.children) {
		panic("Index can be at most len(b.children)")
	}

	nExistingChildren := len(b.children)
	nChildren := len(children)

	tmp := make([]*BNode, nExistingChildren+nChildren)
	copy(tmp[:index], b.children[:index])
	copy(tmp[index:index+nChildren], children)
	copy(tmp[nChildren+index:], b.children[index:])

	b.children = tmp
}

// TODO: TEST
func (b *BNode) predecessorKeyNode(k string) *BNode {
	index, exists := b.keyIndex(k)
	if !exists {
		return nil
	}

	return b.children[index]
}

// TODO: TEST
func (b *BNode) successorKeyNode(k string) *BNode {
	index, exists := b.keyIndex(k)
	if !exists {
		return nil
	}

	return b.children[index+1]
}

func (b *BNode) childPredecessor(index int) *BNode {
	if index <= 0 {
		return nil
	}
	return b.children[index-1]
}

func (b *BNode) childSuccessor(index int) *BNode {
	if index >= len(b.children)-1 {
		return nil
	}
	return b.children[index+1]
}

func (b *BNode) deleteKey(k string) error {
	index, exists := b.keyIndex(k)
	if exists && b.leaf {
		b.records = append(b.records[:index], b.records[index+1:]...)
		return nil
	} else if exists {
		// INTERNAL NODES
		// NOT LEAF
		// Case 1: Predcessor has at least t keys
		if p := b.predecessorKeyNode(k); p != nil && !p.isSparse() {
			pred_k := p.records.last()
			b.records[index] = pred_k
			return p.deleteKey(pred_k.key)
			// Case 2: Successor has at least t keys
		} else if s := b.successorKeyNode(k); s != nil && !s.isSparse() {
			succ_k := s.records[0]
			b.records[index] = succ_k
			return s.deleteKey(succ_k.key)
			// Case 3: Neither p nor s has >= t keys
		} else {
			// Merge s and p with k as median key
			b.mergeChildren(index)
			b.children[index].deleteKey(k)
		}

		return nil
	} else {
		return fmt.Errorf("KeyNotFoundError")
	}
}

// TODO: TEST
func (b *BNode) hasKey(k string) bool {
	_, exists := b.keyIndex(k)
	return exists
}

func (b *BNode) isSparse() bool {
	return len(b.records) <= b.t-1
}

func (b *BNode) mergeChildren(index int) {
	var (
		pivotRecord = b.records[index]
		leftChild   = b.children[index]
		rightChild  = b.children[index+1]
	)

	node := newNode(b.t)
	node.leaf = leftChild.leaf

	node.records = append(node.records, leftChild.records...)
	node.records = append(node.records, pivotRecord)
	node.records = append(node.records, rightChild.records...)

	node.children = append(node.children, leftChild.children...)
	node.children = append(node.children, rightChild.children...)

	// Delete the key from the node
	b.records = append(b.records[:index], b.records[index+1:]...)

	b.children[index] = node
	b.children = append(b.children[:index+1], b.children[index+2:]...)
}
