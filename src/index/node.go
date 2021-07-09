package index

import (
	"fmt"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/repository"
)

type Record struct {
	Key   string
	Value string
}

func (d Record) IsLessThan(other Record) bool {
	return strings.Compare(d.Key, other.Key) < 0
}

func (d Record) IsEqualTo(other Record) bool {
	return d.Key == other.Key
}

// A Node is an implementation of a node in a B-tree.
type Node struct {
	Name     string
	Children []string
	Docs     []Record
	Leaf     bool
	T        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).

	repo repository.Repository
}

func (n *Node) ID() string {
	return n.Name
}

func (n *Node) child(i int) (*Node, error) {
	const op errors.Op = "(*Page).Child"

	if got := len(n.Children); i >= got {
		return nil, errors.Wrap(op, errors.EInternal, fmt.Errorf("OutOfBounds %d (length %d)", i, got))
	}

	item, err := n.repo.Get(n.Children[i])
	if err != nil {
		return nil, err
	}

	child, ok := item.(*Node)
	if !ok {
		return nil, fmt.Errorf("(*Page).child: Could not load page")
	}

	return child, nil
}

// Delete record with key `k` from page `p` if it exists.
// Returns an error otherwise.
func (p *Node) remove(k string) error {
	const op errors.Op = "(*page).Delete"
	index, exists := p.keyIndex(k)
	if !exists {
		return errors.NewKeyNotFoundError(op, k)
	}

	if p.Leaf {
		p.Docs = append(p.Docs[:index], p.Docs[index+1:]...)
		p.save()
		return nil
	}

	// Case 1: Predcessor has at least t keys
	beforeChild, err := p.child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !beforeChild.sparse() {
		//maxPredPage, err := beforeChild.maxPage().forEach(handleSparsePage).Get()
		//if err != nil {
		//return errors.Wrap(op, errors.EInternal, err)
		//}

		//predRec := maxPredPage.docs[len(maxPredPage.docs)-1]

		//p.docs[index] = predRec
		p.save()

		//return maxPredPage.remove(predRec.Key)
		return nil
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

		succRec := succ.Docs[0]

		p.Docs[index] = succRec
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
func (n *Node) insert(d Record) error {
	out := []Record{}

	for i, key := range n.Docs {
		if d.Key == key.Key {
			n.Docs[i] = d
			n.save()
			return nil
		}

		if d.IsLessThan(key) {
			if n.full() {
				panic(fmt.Errorf("Cannot insert key into full node: %s", key.Key))
			}

			out = append(out, d)
			n.Docs = append(out, n.Docs[i:]...)
			n.save()
			return nil
		}

		out = append(out, n.Docs[i])
	}

	if n.full() {
		panic(fmt.Sprintf("Cannot insert key into full node: %s", d.Key))
	}

	n.Docs = append(out, d)
	return n.save()
}

// full reports whether the number of records contained in a
// node equals 2*`b.T`-1
func (p *Node) full() bool {
	return len(p.Docs) == 2*p.T-1
}

// sparse reports whether the number of records contained in
// the node is less than or equal to `b`.T-1
func (p *Node) sparse() bool {
	return len(p.Docs) <= p.T-1
}

// empty reports whether the node is empty (i.e., has no
// records).
func (p *Node) empty() bool {
	return len(p.Docs) == 0
}

func (p *Node) newPage(leaf bool) (*Node, error) {
	item := p.repo.New()

	page, ok := item.(*Node)
	if !ok {
		return nil, fmt.Errorf("Could not create new page")
	}

	page.Leaf = leaf

	return page, nil
}

// keyIndex returns the index of key k in node b if it
// exists. Otherwise, it returns the index of the subtree
// where the key could be possibly be found
func (p *Node) keyIndex(k string) (index int, exists bool) {
	for i, kv := range p.Docs {
		if k == kv.Key {
			return i, true
		}

		if strings.Compare(k, kv.Key) < 0 {
			return i, false
		}
	}

	return len(p.Docs), false
}

// Panics if child is not full
func (p *Node) splitChild(index int) error {
	const op errors.Op = "(*Page).splitChild"

	fullChild, err := p.child(index)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if !fullChild.full() {
		return errors.Wrap(op, errors.EInternal, fmt.Errorf("Cannot split non-full child"))
	}

	newChild, err := p.newPage(fullChild.Leaf)
	if err != nil {
		return err
	}

	medianKey, left, right := partitionMedian(fullChild.Docs)
	p.insert(medianKey)

	fullChild.Docs, newChild.Docs = left, right

	if !fullChild.Leaf {
		newChild.insertChildren(0, fullChild.Children[p.T:]...)
		fullChild.Children = fullChild.Children[:p.T]
	}

	p.insertChildren(index+1, newChild.ID())

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

func (p *Node) setRecord(index int, d Record) {
	p.Docs[index] = d
}

func (p *Node) insertChildren(index int, children ...string) {
	nExistingChildren := len(p.Children)
	nChildren := len(children)

	tmp := make([]string, nExistingChildren+nChildren)
	copy(tmp[:index], p.Children[:index])
	copy(tmp[index:index+nChildren], children)
	copy(tmp[nChildren+index:], p.Children[index:])

	p.Children = tmp
}

func (p *Node) predecessorPage(k string) (*Node, error) {
	const op errors.Op = "(*Page).predecessorPage"
	if p.Leaf {
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

func (p *Node) successorPage(k string) (*Node, error) {
	const op errors.Op = "(*Page).successorPage"

	if p.Leaf {
		return nil, errors.Wrap(op, errors.EInternal, fmt.Errorf("Leaf has no successor page"))
	}

	index, exists := p.keyIndex(k)
	if !exists {
		return nil, errors.NewKeyNotFoundError(op, k)
	}

	page, err := p.child(index + 1)
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	return page.minPage().Get()
}

// TODO: return error
func (p *Node) prevChildSibling(index int) *Node {
	if index <= 0 {
		return nil
	}
	child, err := p.child(index - 1)
	if err != nil {
		return nil
	}

	return child
}

func (p *Node) nextChildSibling(index int) *Node {
	if index >= len(p.Children)-1 {
		return nil
	}
	child, err := p.child(index + 1)
	if err != nil {
		return nil
	}

	return child
}

// TODO: TEST
func (p *Node) mergeWith(median Record, other *Node) {
	p.Docs = append(p.Docs, median)
	p.Docs = append(p.Docs, other.Docs...)
	p.Children = append(p.Children, other.Children...)
}

// TODO: Errors
// mergeChildren merges the child at index `i` of `b` with
// the child at index `i+1` of `b`, inserting the key at
// index `i` as the median key and removing the key from `b` in
// the process. The original sibling node (i+1) is scheduled
// for deletion.
func (p *Node) mergeChildren(i int) {
	var (
		pivotDoc      = p.Docs[i]
		leftChild, _  = p.child(i)
		rightChild, _ = p.child(i + 1)
	)

	leftChild.mergeWith(pivotDoc, rightChild)

	// Delete the key from the node
	p.Docs = append(p.Docs[:i], p.Docs[i+1:]...)
	// Remove rightChild
	p.Children = append(p.Children[:i+1], p.Children[i+2:]...)

	p.save()
	leftChild.save()
	rightChild.deletePage()
}

// TODO: return error
func partitionMedian(nums []Record) (Record, []Record, []Record) {
	if nDocs := len(nums); nDocs%2 == 0 || nDocs < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

// TODO: return error
func handleSparsePage(node, child *Node) {
	if !child.sparse() {
		return
	}

	//index, ok := node.childIndex(child)
	//if !ok {
	//panic("Tried to find childIndex of invalid child")
	//}

	//var (
	//p = node.prevChildSibling(index)
	//s = node.nextChildSibling(index)
	//)

	//// Rotate predecessor key
	//if p != nil && !p.sparse() {
	//var (
	//recordIndex   = index - 1
	//pivot         = node.docs[recordIndex]
	//siblingRecord = p.docs[len(p.docs)-1]
	//)

	//child.insert(pivot)
	//node.setRecord(recordIndex, siblingRecord)

	//if !p.leaf {
	//// Move child from sibling to child
	//siblingLastChild := p.children[len(p.children)-1]
	//child.children = append([]*Page{siblingLastChild}, child.children...)
	//p.children = p.children[:len(p.children)-1]
	//}
	//p.save()
	//} else if s != nil && !s.sparse() {
	//var (
	//pivot         = node.docs[index]
	//siblingRecord = s.docs[0]
	//)

	//// Move key from parent to child
	//child.docs = append(child.docs, pivot)
	//node.setRecord(index, siblingRecord)

	//// Move child from sibling to child
	//if !s.leaf {
	//siblingFirstChild := s.children[0]
	//child.children = append(child.children, siblingFirstChild)
	//s.children = s.children[1:]
	//}
	//s.save()
	//} else if p != nil {
	//node.mergeChildren(index - 1)
	//} else {
	//node.mergeChildren(index)
	//}

	//child.save()
	//node.save()
}

// TODO: Return error
func splitFullPage(node, child *Node) {
	if !child.full() {
		return
	}

	index, ok := node.childIndex(child)
	if !ok {
		panic("Tried to find childIndex of invalid child")
	}

	node.splitChild(index)
}

func (p *Node) childIndex(c *Node) (int, bool) {
	for i, child := range p.Children {
		if child == c.ID() {
			return i, true
		}
	}

	return 0, false
}

func (p *Node) save() error {
	return p.repo.Save(p)
}

func (p *Node) deletePage() error {
	return p.repo.Delete(p)
}

func (p Node) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "-----\nPage\n-----\n")
	if p.Name != "" {
		fmt.Fprintf(&sb, "ID:\t\t%s\n", p.Name)
	} else {
		fmt.Fprint(&sb, "ID:\t\t<NONE>\n")
	}
	fmt.Fprintf(&sb, "t:\t\t%d\n", p.T)
	fmt.Fprintf(&sb, "Leaf:\t\t%v\n", p.Leaf)
	fmt.Fprintf(&sb, "Children:\t%v\n", len(p.Children))
	fmt.Fprintf(&sb, "Docs:\t")
	for _, r := range p.Docs {
		fmt.Fprintf(&sb, "%v ", r)
	}
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
