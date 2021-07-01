package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	record "github.com/namvu9/keylime/src/types"
)

// A Page is an implementation of a node in a B-tree.
type Page struct {
	ID string

	children []*Page
	records  []record.Record
	loaded   bool
	leaf     bool
	t        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).

	reader io.Reader
	writer *WriteBuffer
}

func (p *Page) Get(k string) ([]byte, error) {
	const op errors.Op = "(*page).Get"

	index, exists := p.keyIndex(k)
	if !exists {
		return nil, errors.NewKeyNotFoundError(op, k)
	}

	return p.records[index].Value, nil
}

func (p *Page) Child(i int) (*Page, error) {
	const op errors.Op = "(*Page).Child"

	if got := len(p.children); i >= got {
		return nil, errors.Wrap(op, errors.EInternal, fmt.Errorf("OutOfBounds %d (length %d)", i, got))
	}

	child := p.children[i]
	if !child.loaded {
		child.load()
	}

	return child, nil
}

// Delete record with key `k` from page `p` if it exists.
// Returns an error otherwise.
func (p *Page) Delete(k string) error {
	const op errors.Op = "(*page).Delete"
	index, exists := p.keyIndex(k)
	if !exists {
		return errors.NewKeyNotFoundError(op, k)
	}

	if p.leaf {
		p.records = append(p.records[:index], p.records[index+1:]...)
		p.save()
		return nil
	}

	// Case 1: Predcessor has at least t keys
	beforeChild, err := p.Child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !beforeChild.Sparse() {
		maxPredPage, err := beforeChild.maxPage().forEach(handleSparsePage).Get()
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		predRec := maxPredPage.records[len(maxPredPage.records)-1]

		p.records[index] = predRec
		p.save()

		return maxPredPage.Delete(predRec.Key)
	}

	// Case 2: Successor has at least t keys
	afterChild, err := p.Child(index + 1)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !afterChild.Sparse() {
		succ, err := afterChild.minPage().forEach(handleSparsePage).Get()
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		succRec := succ.records[0]

		p.records[index] = succRec
		p.save()

		return succ.Delete(succRec.Key)
	}

	// Case 3: Neither p nor s has >= t keys
	// Merge s and p with k as median key
	p.mergeChildren(index)
	p.save()

	deleteChild, err := p.Child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return deleteChild.Delete(k)
}

// insert key `k` into node `b` in sorted order. Panics if node is full. Returns the index at which the key was inserted
func (p *Page) insert(r record.Record) int {
	out := []record.Record{}

	for i, key := range p.records {
		if r.Key == key.Key {
			p.records[i] = r
			p.save()
			return i
		}

		if r.IsLessThan(key) {
			if p.Full() {
				panic(fmt.Errorf("Cannot insert key into full node: %s", key.Key))
			}

			out = append(out, r)
			p.records = append(out, p.records[i:]...)
			p.save()
			return i
		}

		out = append(out, p.records[i])
	}

	if p.Full() {
		panic(fmt.Sprintf("Cannot insert key into full node: %s", r.Key))
	}

	p.records = append(out, r)
	p.save()

	return len(p.records) - 1
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

func (p *Page) newPage(leaf bool) *Page {
	np := newPage(p.t, leaf, p.writer)
	return np
}

func (p *Page) newPageWithID(id string) *Page {
	return newPageWithID(p.t, id, p.writer)

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

// Panics if child is not full
func (p *Page) splitChild(index int) error {
	const op errors.Op = "(*Page).splitChild"

	fullChild, err := p.Child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !fullChild.Full() {
		return errors.Wrap(op, errors.EInternal, fmt.Errorf("Cannot split non-full child"))
	}

	newChild := p.newPage(fullChild.leaf)

	medianKey, left, right := partitionMedian(fullChild.records)
	p.insert(medianKey)

	fullChild.records, newChild.records = left, right

	if !fullChild.leaf {
		newChild.insertChildren(0, fullChild.children[p.t:]...)
		fullChild.children = fullChild.children[:p.t]
	}

	p.insertChildren(index+1, newChild)

	err = newChild.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = fullChild.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = p.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return nil
}

func (p *Page) setRecord(index int, r record.Record) {
	p.records[index] = r
}

func (p *Page) insertChildren(index int, children ...*Page) {
	if p.Full() {
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

func (p *Page) predecessorPage(k string) (*Page, error) {
	const op errors.Op = "(*Page).predecessorPage"
	if p.leaf {
		return nil, errors.Wrap(op, errors.EInternal, fmt.Errorf("Leaf has no predecessor page"))
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil, errors.NewKeyNotFoundError(op, k)
	}

	child, err := p.Child(index)
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	page, err := child.maxPage().Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	return page, nil
}

func (p *Page) successorPage(k string) (*Page, error) {
	const op errors.Op = "(*Page).successorPage"

	if p.leaf {
		return nil, errors.Wrap(op, errors.EInternal, fmt.Errorf("Leaf has no successor page"))
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil, errors.NewKeyNotFoundError(op, k)
	}

	page, err := p.children[index+1].minPage().Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	return page, nil
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

// TODO: Errors
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

	p.save()
	leftChild.save()
	rightChild.deletePage()
}

// TODO: return error
func partitionMedian(nums []record.Record) (record.Record, []record.Record, []record.Record) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

// TODO: return error
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

		child.insert(pivot)
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
		node.mergeChildren(index - 1)
	} else {
		node.mergeChildren(index)
	}

	child.save()
	node.save()
}

// TODO: Return error
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

func (p *Page) save() error {

	var op errors.Op = "(*Page).save"

	err := p.writer.Write(p)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	return nil
}

func (p *Page) deletePage() error {
	var op errors.Op = "(*Page).deletePage"

	err := p.writer.Delete(p)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	return nil
}

type PageSerialized struct {
	ID string

	Children []string
	Records  []record.Record
	loaded   bool
	Leaf     bool
	T        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).
}

func (p *Page) load() error {
	var op errors.Op = "(*Page).load"

	data, err := io.ReadAll(p.reader)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}
	buf := bytes.NewBuffer(data)
	ps := PageSerialized{}

	dec := gob.NewDecoder(buf)
	err = dec.Decode(&ps)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	ps.ToDeserialized(p)

	return nil
}

func (p *Page) ToSerialized() *PageSerialized {
	children := []string{}

	for _, child := range p.children {
		children = append(children, child.ID)
	}

	return &PageSerialized{
		ID:       p.ID,
		Records:  p.records,
		Leaf:     p.leaf,
		T:        p.t,
		Children: children,
	}
}

func (ps *PageSerialized) ToDeserialized(p *Page) {
	p.ID = ps.ID
	p.records = ps.Records
	p.leaf = ps.Leaf
	p.t = ps.T

	children := []*Page{}

	for _, childID := range ps.Children {
		if childID == ps.ID {
			panic(fmt.Sprintf("Page %s has a child reference to itself", ps.ID))
		}

		np := p.newPageWithID(childID)
		np.loaded = false
		children = append(children, np)
	}

	p.children = children
	p.loaded = true
}
