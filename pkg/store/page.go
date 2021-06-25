package store

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/pkg/record"
)

// A Page is an implementation of a node in a B-tree.
type Page struct {
	ID string

	loaded   bool
	children []*Page
	records  []record.Record
	leaf     bool
	t        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).

	c *Collection
}

func (p *Page) Get(k string) ([]byte, error) {
	index, ok := p.keyIndex(k)
	if !ok {
		return nil, errors.New("KeyNotFound")
	}

	return p.records[index].Value, nil
}

// Delete record with key `k` from page `p` if it exists.
// Returns an error otherwise.
func (p *Page) Delete(k string) error {
	index, exists := p.keyIndex(k)
	if !exists {
		return fmt.Errorf("KeyNotFoundError")
	}

	if p.leaf {
		p.records = append(p.records[:index], p.records[index+1:]...)
		p.save()
	}

	// Case 1: Predcessor has at least t keys
	if beforeChild := p.children[index]; !beforeChild.Sparse() {
		var (
			maxPredPage = beforeChild.maxPage().forEach(handleSparsePage).Get()
			predRec     = maxPredPage.records[len(maxPredPage.records)-1]
		)

		p.records[index] = predRec
		p.save()

		return maxPredPage.Delete(predRec.Key)
	}

	// Case 2: Successor has at least t keys
	if afterChild := p.children[index+1]; !afterChild.Sparse() {
		var (
			succ    = afterChild.minPage().forEach(handleSparsePage).Get()
			succRec = succ.records[0]
		)

		p.records[index] = succRec
		p.save()

		return succ.Delete(succRec.Key)
	}

	// Case 3: Neither p nor s has >= t keys
	// Merge s and p with k as median key
	p.mergeChildren(index)
	p.save()

	return p.children[index].Delete(k)
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

// Leaf returns true if `p` is a leaf node
func (p *Page) Leaf() bool {
	return p.leaf
}

func (p *Page) newPage() *Page {
	np := newPage(p.t)
	np.c = p.c
	return np
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
func (p *Page) keyIndex(k string) (index int, exists bool) {
	for i, kv := range p.records {
		if k == kv.Key {
			return i, true
		}

		if strings.Compare(k, kv.Key) < 0 {
			return i, false
		}
	}

	return len(p.records), false
}

// insert key `k` into node `b` in sorted order. Panics if node is full. Returns the index at which the key was inserted
func (p *Page) insert(k string, value []byte) int {
	if p.Full() {
		panic("Cannot insert key into full node")
	}

	kv := record.New(k, value)
	out := []record.Record{}

	for i, key := range p.records {
		if kv.Key == key.Key {
			p.records[i] = kv
			p.save()
			return i
		}

		if kv.IsLessThan(key) {
			out = append(out, kv)
			p.records = append(out, p.records[i:]...)
			p.save()
			return i
		}

		out = append(out, p.records[i])
	}

	p.records = append(out, kv)
	p.save()

	return len(p.records) - 1
}

// Panics if child is not full
func (p *Page) splitChild(index int) {
	fullChild := p.children[index]
	if !fullChild.Full() {
		panic("Cannot split non-full child")
	}


	newChild := p.newPage()
	newChild.leaf = fullChild.leaf

	medianKey, left, right := partitionMedian(fullChild.records)
	p.insert(medianKey.Key, medianKey.Value)

	fullChild.records, newChild.records = left, right

	if !fullChild.leaf {
		newChild.insertChildren(0, fullChild.children[p.t:]...)
		fullChild.children = fullChild.children[:p.t]
	}

	p.insertChildren(index+1, newChild)

	newChild.save()
	fullChild.save()
	p.save()
}

func (p *Page) insertRecord(r record.Record) int {
	return p.insert(r.Key, r.Value)
}

func (p *Page) setRecord(index int, r record.Record) {
	p.records[index] = r
}

func (p *Page) insertChildren(index int, children ...*Page) {
	if len(p.children) == 2*p.t {
		panic("Cannot insert a child into a full node")
	}

	// Check whether index + len(children) leads to node
	// overflow
	nExistingChildren := len(p.children)
	nChildren := len(children)

	tmp := make([]*Page, nExistingChildren+nChildren)
	copy(tmp[:index], p.children[:index])
	copy(tmp[index:index+nChildren], children)
	copy(tmp[nChildren+index:], p.children[index:])

	p.children = tmp
}

func (p *Page) predecessorPage(k string) *Page {
	if p.leaf {
		return nil
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil
	}

	return p.children[index].maxPage().Get()
}

func (p *Page) successorPage(k string) *Page {
	if p.leaf {
		return nil
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil
	}

	return p.children[index+1].minPage().Get()
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
func (p *Page) mergeWith(median record.Record, other *Page) {
	p.records = append(p.records, median)
	p.records = append(p.records, other.records...)
	p.children = append(p.children, other.children...)
}

// mergeChildren merges the child at index `i` of `b` with
// the child at index `i+1` of `b`, inserting the key at
// index `i` as the median key and removing the key from `b` in
// the process. The original sibling node (i+1) is scheduled
// for deletion.
func (p *Page) mergeChildren(i int) {
	var (
		pivotRecord = p.records[i]
		leftChild   = p.children[i]
		rightChild  = p.children[i+1]
	)

	leftChild.mergeWith(pivotRecord, rightChild)

	// Delete the key from the node
	p.records = append(p.records[:i], p.records[i+1:]...)
	// Remove rightChild
	p.children = append(p.children[:i+1], p.children[i+2:]...)
}

func partitionMedian(nums []record.Record) (record.Record, []record.Record, []record.Record) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

func handleSparsePage(node, child *Page) {
	if !child.Sparse() {
		return
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
		p.save()
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
		s.save()
	} else if p != nil {
		// TODO: Delete other node
		node.mergeChildren(index - 1)
	} else {
		node.mergeChildren(index)
	}

	node.save()
}

func splitFullPage(node, child *Page) {
	if !child.Full() {
		return
	}

	index, ok := node.childIndex(child)
	if !ok {
		panic("Tried to find childIndex of invalid child")
	}

	node.splitChild(index)
}

func (p *Page) childIndex(c *Page) (int, bool) {
	for i, child := range p.children {
		if child == c {
			return i, true
		}
	}

	return 0, false
}

func (p *Page) save() {
	p.c.writePage(p)
}

func (p *Page) load() error {
	return p.c.loadPage(p)
}
