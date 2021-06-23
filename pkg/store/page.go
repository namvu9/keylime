package store

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/pkg/record"
)

type Page struct {
	ID string

	loaded   bool
	children []*Page
	records  []record.Record
	leaf     bool
	t        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).
}

func (b *Page) Get(k string) ([]byte, error) {
	index, ok := b.keyIndex(k)
	if !ok {
		return nil, errors.New("KeyNotFound")
	}

	return b.records[index].Value(), nil
}

func (b *Page) Delete(k string) error {
	index, exists := b.keyIndex(k)
	if !exists {
		return fmt.Errorf("KeyNotFoundError")
	}

	if b.leaf {
		b.records = append(b.records[:index], b.records[index+1:]...)
		return nil
	}

	// Case 1: Predcessor has at least t keys
	if beforeChild := b.children[index]; !beforeChild.Sparse() {
		var (
			maxPredPage = beforeChild.MaxPage().forEach(handleSparsePage)
			predRec     = maxPredPage.records[len(maxPredPage.records)-1]
		)

		b.records[index] = predRec
		return maxPredPage.Delete(predRec.Key())
	}

	// Case 2: Successor has at least t keys
	if afterChild := b.children[index+1]; !afterChild.Sparse() {
		var (
			minSuccPage = afterChild.MinPage().forEach(handleSparsePage)
			succRec     = minSuccPage.records[0]
		)

		b.records[index] = succRec
		return minSuccPage.Delete(succRec.Key())
	}

	// Case 3: Neither p nor s has >= t keys
	// Merge s and p with k as median key
	b.mergeChildren(index)
	return b.children[index].Delete(k)
}

// Full reports whether the number of records contained in a
// node equals 2*`b.T`-1
func (p *Page) Full() bool {
	return len(p.records) == 2*p.t-1
}

// Sparse reports whether the number of records contained in
// the node is less than or equal to `b`.T-1
func (p *Page) Sparse() bool {
	return len(p.records) <= p.t-1
}

// Empty reports whether the node is empty (i.e., has no
// records).
func (p *Page) Empty() bool {
	return len(p.records) == 0
}

func (p *Page) Leaf() bool {
	return p.leaf
}

func (p *Page) newPage() *Page {
	return newPage(p.t)
}

func newPage(t int) *Page {
	return &Page{
		ID:       uuid.New().String(),
		children: []*Page{},
		records:  make([]record.Record, 0, 2*t-1),
		leaf:     false,
		t:        t,
	}
}

// keyIndex returns the index of key k in node b if it
// exists. Otherwise, it returns the index of the subtree
// where the key could be possibly be found
func (b *Page) keyIndex(k string) (index int, exists bool) {
	for i, kv := range b.records {
		if k == kv.Key() {
			return i, true
		}

		if strings.Compare(k, kv.Key()) < 0 {
			return i, false
		}
	}

	return len(b.records), false
}

// insertKey key `k` into node `b` in sorted order. Panics if node is full. Returns the index at which the key was inserted
func (b *Page) insertKey(k string, value []byte) int {
	if b.Full() {
		panic("Cannot insert key into full node")
	}

	kv := record.New(k, value)
	out := []record.Record{}

	for i, key := range b.records {
		if kv.Key() == key.Key() {
			b.records[i] = kv
			return i
		}

		if kv.IsLessThan(key) {
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
func (b *Page) splitChild(index int) {
	fullChild := b.children[index]
	if !fullChild.Full() {
		panic("Cannot split non-full child")
	}

	newChild := b.newPage()
	newChild.leaf = fullChild.leaf

	medianKey, left, right := partitionMedian(fullChild.records)
	b.insertKey(medianKey.Key(), medianKey.Value())

	fullChild.records, newChild.records = left, right

	if !fullChild.leaf {
		newChild.insertChildren(0, fullChild.children[b.t:]...)
		fullChild.children = fullChild.children[:b.t]
	}

	b.insertChildren(index+1, newChild)
}

func (b *Page) insertRecord(r record.Record) int {
	return b.insertKey(r.Key(), r.Value())
}

func (b *Page) setRecord(index int, r record.Record) {
	b.records[index] = r
}

func (b *Page) insertChildren(index int, children ...*Page) {
	if len(b.children) == 2*b.t {
		panic("Cannot insert a child into a full node")
	}

	// Check whether index + len(children) leads to node
	// overflow
	nExistingChildren := len(b.children)
	nChildren := len(children)

	tmp := make([]*Page, nExistingChildren+nChildren)
	copy(tmp[:index], b.children[:index])
	copy(tmp[index:index+nChildren], children)
	copy(tmp[nChildren+index:], b.children[index:])

	b.children = tmp
}

func (p *Page) predecessorNode(k string) *Page {
	if p.leaf {
		return nil
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil
	}

	return p.children[index].MaxPage().Get()
}

func (p *Page) successorNode(k string) *Page {
	if p.leaf {
		return nil
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil
	}

	return p.children[index+1].MinPage().Get()
}

func (p *Page) prevChildSibling(index int) *Page {
	if index <= 0 {
		return nil
	}
	return p.children[index-1]
}

func (p *Page) nextChildSibling(index int) *Page {
	if index >= len(p.children)-1 {
		return nil
	}
	return p.children[index+1]
}

// TODO: TEST
func (b *Page) hasKey(k string) bool {
	_, exists := b.keyIndex(k)
	return exists
}

// TODO: TEST
func (b *Page) mergeWith(median record.Record, other *Page) {
	b.records = append(b.records, median)
	b.records = append(b.records, other.records...)
	b.children = append(b.children, other.children...)
}

// mergeChildren merges the child at index `i` of `b` with
// the child at index `i+1` of `b`, inserting the key at
// index `i` as the median key and removing the key from `b` in
// the process. The original sibling node (i+1) is scheduled
// for deletion.
func (b *Page) mergeChildren(i int) {
	var (
		pivotRecord = b.records[i]
		leftChild   = b.children[i]
		rightChild  = b.children[i+1]
	)

	leftChild.mergeWith(pivotRecord, rightChild)

	// Delete the key from the node
	b.records = append(b.records[:i], b.records[i+1:]...)
	// Remove rightChild
	b.children = append(b.children[:i+1], b.children[i+2:]...)
}

func (p *Page) read() error {
	return nil
}

func partitionMedian(nums []record.Record) (record.Record, []record.Record, []record.Record) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

// TODO: TEST
func handleSparsePage(node, child *Page) bool {
	if !child.Sparse() {
		return false
	}

	index, ok := node.childIndex(child)
	if !ok {
		panic("Tried to find childIndex of invalid child")
	}

	var (
		p = node.prevChildSibling(index)
		s = node.nextChildSibling(index)
	)

	// Rotate predecessor key
	if p != nil && !p.Sparse() {
		var (
			recordIndex   = index - 1
			pivot         = node.records[recordIndex]
			siblingRecord = p.records[len(p.records)-1]
		)

		child.insertRecord(pivot)
		node.setRecord(recordIndex, siblingRecord)

		if !p.leaf {
			// Move child from sibling to child
			siblingLastChild := p.children[len(p.children)-1]
			child.children = append([]*Page{siblingLastChild}, child.children...)
			p.children = p.children[:len(p.children)-1]
		}
	} else if s != nil && !s.Sparse() {
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

	return true
}

func splitFullPage(node, child *Page) bool {
	if !child.Full() {
		return false
	}

	index, ok := node.childIndex(child)
	if !ok {
		panic("Tried to find childIndex of invalid child")
	}

	node.splitChild(index)

	return true
}

func (p *Page) childIndex(c *Page) (int, bool) {
	for i, child := range p.children {
		if child == c {
			return i, true
		}
	}

	return 0, false
}
