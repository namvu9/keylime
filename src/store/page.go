package store

import (
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/types"
)

type DocRef struct {
	Key     string
	BlockID string
}

// A Page is an implementation of a node in a B-tree.
type Page struct {
	ID       string
	Children []string
	Docs     []DocRef
	loaded   bool
	leaf     bool
	t        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).

	children []*Page
	docs     []types.Document

	reader io.Reader
}

func (p *Page) child(i int) (*Page, error) {
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
func (p *Page) remove(k string) error {
	const op errors.Op = "(*page).Delete"
	index, exists := p.keyIndex(k)
	if !exists {
		return errors.NewKeyNotFoundError(op, k)
	}

	if p.leaf {
		p.docs = append(p.docs[:index], p.docs[index+1:]...)
		p.save()
		return nil
	}

	// Case 1: Predcessor has at least t keys
	beforeChild, err := p.child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !beforeChild.sparse() {
		maxPredPage, err := beforeChild.maxPage().forEach(handleSparsePage).Get()
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		predRec := maxPredPage.docs[len(maxPredPage.docs)-1]

		p.docs[index] = predRec
		p.save()

		return maxPredPage.remove(predRec.Key)
	}

	// Case 2: Successor has at least t keys
	afterChild, err := p.child(index + 1)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !afterChild.sparse() {
		succ, err := afterChild.minPage().forEach(handleSparsePage).Get()
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		succRec := succ.docs[0]

		p.docs[index] = succRec
		p.save()

		return succ.remove(succRec.Key)
	}

	// Case 3: Neither p nor s has >= t keys
	// Merge s and p with k as median key
	p.mergeChildren(index)
	p.save()

	deleteChild, err := p.child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return deleteChild.remove(k)
}

// insert key `k` into node `b` in sorted order. Panics if node is full. Returns the index at which the key was inserted
func (p *Page) insert(r types.Document) int {
	out := []types.Document{}

	for i, key := range p.docs {
		if r.Key == key.Key {
			p.docs[i] = r
			p.save()
			return i
		}

		if r.IsLessThan(key) {
			if p.full() {
				panic(fmt.Errorf("Cannot insert key into full node: %s", key.Key))
			}

			out = append(out, r)
			p.docs = append(out, p.docs[i:]...)
			p.save()
			return i
		}

		out = append(out, p.docs[i])
	}

	if p.full() {
		panic(fmt.Sprintf("Cannot insert key into full node: %s", r.Key))
	}

	p.docs = append(out, r)
	p.save()

	return len(p.docs) - 1
}

// full reports whether the number of records contained in a
// node equals 2*`b.T`-1
func (p *Page) full() bool {
	return len(p.docs) == 2*p.t-1
}

// sparse reports whether the number of records contained in
// the node is less than or equal to `b`.T-1
func (p *Page) sparse() bool {
	return len(p.docs) <= p.t-1
}

// empty reports whether the node is empty (i.e., has no
// records).
func (p *Page) empty() bool {
	return len(p.docs) == 0
}

func (p *Page) newPage(leaf bool) *Page {
	return nil
}

func (p *Page) newPageWithID(id string) *Page {
	return newPageWithID(p.t, id)
}

// keyIndex returns the index of key k in node b if it
// exists. Otherwise, it returns the index of the subtree
// where the key could be possibly be found
func (p *Page) keyIndex(k string) (index int, exists bool) {
	for i, kv := range p.docs {
		if k == kv.Key {
			return i, true
		}

		if strings.Compare(k, kv.Key) < 0 {
			return i, false
		}
	}

	return len(p.docs), false
}

// Panics if child is not full
func (p *Page) splitChild(index int) error {
	const op errors.Op = "(*Page).splitChild"

	fullChild, err := p.child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !fullChild.full() {
		return errors.Wrap(op, errors.EInternal, fmt.Errorf("Cannot split non-full child"))
	}

	newChild := p.newPage(fullChild.leaf)

	medianKey, left, right := partitionMedian(fullChild.docs)
	p.insert(medianKey)

	fullChild.docs, newChild.docs = left, right

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

func (p *Page) setRecord(index int, r types.Document) {
	p.docs[index] = r
}

func (p *Page) insertChildren(index int, children ...*Page) {
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

	child, err := p.child(index)
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
func (p *Page) mergeWith(median types.Document, other *Page) {
	p.docs = append(p.docs, median)
	p.docs = append(p.docs, other.docs...)
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
		pivotRecord = p.docs[i]
		leftChild   = p.children[i]
		rightChild  = p.children[i+1]
	)

	leftChild.mergeWith(pivotRecord, rightChild)

	// Delete the key from the node
	p.docs = append(p.docs[:i], p.docs[i+1:]...)
	// Remove rightChild
	p.children = append(p.children[:i+1], p.children[i+2:]...)

	p.save()
	leftChild.save()
	rightChild.deletePage()
}

// TODO: return error
func partitionMedian(nums []types.Document) (types.Document, []types.Document, []types.Document) {
	if nDocs := len(nums); nDocs%2 == 0 || nDocs < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

// TODO: return error
func handleSparsePage(node, child *Page) {
	if !child.sparse() {
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
	if p != nil && !p.sparse() {
		var (
			recordIndex   = index - 1
			pivot         = node.docs[recordIndex]
			siblingRecord = p.docs[len(p.docs)-1]
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
	} else if s != nil && !s.sparse() {
		var (
			pivot         = node.docs[index]
			siblingRecord = s.docs[0]
		)

		// Move key from parent to child
		child.docs = append(child.docs, pivot)
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
	if !child.full() {
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

	//var op errors.Op = "(*Page).save"

	return nil
}

func (p *Page) deletePage() error {
	//var op errors.Op = "(*Page).deletePage"

	//err := p.writer.Delete(p)
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	return nil
}

func (p *Page) Name() string {
	return p.ID
}

func (p *Page) load() error {
	//var op errors.Op = "(*Page).load"

	//data, err := io.ReadAll(p.reader)
	//if err != nil {
	//return errors.Wrap(op, errors.EInternal, err)
	//}
	//buf := bytes.NewBuffer(data)
	//ps := PageSerialized{}

	//dec := gob.NewDecoder(buf)
	//err = dec.Decode(&ps)
	//if err != nil {
	//return errors.Wrap(op, errors.EInternal, err)
	//}

	//ps.ToDeserialized(p)

	return nil
}

func newPage(t int, leaf bool) *Page {
	id := uuid.New().String()

	p := &Page{
		ID:     id,
		leaf:   leaf,
		t:      t,
		loaded: true,
	}

	return p
}

func newPageWithID(t int, id string) *Page {
	p := &Page{
		ID:     id,
		leaf:   false,
		t:      t,
		loaded: true,
	}

	return p
}
