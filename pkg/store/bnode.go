package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/pkg/record"
)

type NodeReadWriter interface {
	Write(string, []byte) (int, error)
	Read(string, *BNode) error
}

type BNode struct {
	loaded   bool
	ID       string
	children []*BNode
	records  []record.Record
	Leaf     bool
	T        int // Minimum degree `t` represents the minimum branching factor of a node (except the root node).

	storage *ChangeReporter
}

func (b *BNode) newNode() *BNode {
	node := newNode(b.T)
	node.storage = b.storage
	return node
}

func (b *BNode) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "-----\nBNode\n-----\n")
	if b.ID != "" {
		fmt.Fprintf(&sb, "ID:\t\t%s\n", b.ID)
	} else {
		fmt.Fprint(&sb, "ID:\t\t<NONE>\n")
	}
	fmt.Fprintf(&sb, "t:\t\t%d\n", b.T)
	fmt.Fprintf(&sb, "Loaded:\t\t%v\n", b.loaded)
	fmt.Fprintf(&sb, "Leaf:\t\t%v\n", b.Leaf)
	fmt.Fprintf(&sb, "Children:\t%v\n", len(b.children))
	fmt.Fprintf(&sb, "Keys:\t\t")
	for _, key := range b.records {
		fmt.Fprintf(&sb, "%v ", key)
	}
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}

func newNode(t int) *BNode {
	return &BNode{
		ID:       uuid.New().String(),
		children: []*BNode{},
		records:  make([]record.Record, 0, 2*t-1),
		Leaf:     false,
		T:        t,
	}
}

func (b *BNode) isFull() bool {
	return len(b.records) == 2*b.T-1
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
func (b *BNode) insertKey(k string, value []byte) int {
	if b.isFull() {
		panic("Cannot insert key into full node")
	}

	b.registerWrite("INSERT KEY")

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
func (b *BNode) splitChild(index int) {
	fullChild := b.children[index]
	if !fullChild.isFull() {
		panic("Cannot split non-full child")
	}

	newChild := b.newNode()
	newChild.Leaf = fullChild.Leaf

	medianKey, left, right := partitionMedian(fullChild.records)
	b.insertKey(medianKey.Key(), medianKey.Value())

	fullChild.records, newChild.records = left, right

	if !fullChild.Leaf {
		newChild.insertChildren(0, fullChild.children[b.T:]...)
		fullChild.children = fullChild.children[:b.T]
	}

	b.insertChildren(index+1, newChild)

	b.registerWrite("Split child")
	newChild.registerWrite("Split child")
	fullChild.registerWrite("Split child")

}

func (b *BNode) insertRecord(r record.Record) int {
	return b.insertKey(r.Key(), r.Value())
}

func (b *BNode) setRecord(index int, r record.Record) {
	b.records[index] = r
}

func (b *BNode) insertChildren(index int, children ...*BNode) {
	if len(b.children) == 2*b.T {
		panic("Cannot insert a child into a full node")
	}

	// Check whether index + len(children) leads to node
	// overflow
	nExistingChildren := len(b.children)
	nChildren := len(children)

	tmp := make([]*BNode, nExistingChildren+nChildren)
	copy(tmp[:index], b.children[:index])
	copy(tmp[index:index+nChildren], children)
	copy(tmp[nChildren+index:], b.children[index:])

	b.children = tmp
}

func (b *BNode) predecessorKeyNode(k string) *BNode {
	index, exists := b.keyIndex(k)
	if !exists {
		return nil
	}

	return b.children[index]
}

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
	//b.registerWrite("delete key")
	if exists && b.Leaf {
		b.records = append(b.records[:index], b.records[index+1:]...)
		return nil
	} else if exists {
		// INTERNAL NODES
		// Case 1: Predcessor has at least t keys
		if p := b.predecessorKeyNode(k); p != nil && !p.isSparse() {
			pred_k := p.records[len(p.records)-1]
			b.records[index] = pred_k
			return p.deleteKey(pred_k.Key())
			// Case 2: Successor has at least t keys
		} else if s := b.successorKeyNode(k); s != nil && !s.isSparse() {
			succ_k := s.records[0]
			b.records[index] = succ_k
			return s.deleteKey(succ_k.Key())
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
	return len(b.records) <= b.T-1
}

// TODO: TEST
func (b *BNode) mergeWith(median record.Record, other *BNode) {
	b.records = append(b.records, median)
	b.records = append(b.records, other.records...)
	b.children = append(b.children, other.children...)

	b.registerWrite("Merge")
	other.registerDelete("Merge")
}

// mergeChildren merges the child at index `i` of `b` with
// the child at index `i+1` of `b`, inserting the key at
// index `i` as the median key and removing the key from `b` in
// the process. The original sibling node (i+1) is scheduled
// for deletion.
func (b *BNode) mergeChildren(i int) {
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

	b.registerWrite("Merged children")
}

func (b *BNode) GobEncode() ([]byte, error) {
	refs := []*BNode{}
	for _, c := range b.children {
		cNode := new(BNode)
		cNode.ID = c.ID
		refs = append(refs, cNode)
	}
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)

	err := encoder.Encode(refs)
	if err != nil {
		return nil, err
	}
	encoder.Encode(b.ID)
	encoder.Encode(b.T)
	encoder.Encode(b.Leaf)
	encoder.Encode(b.records)

	return w.Bytes(), nil
}

func (b *BNode) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)

	if err := decoder.Decode(&b.children); err != nil {
		return err
	}
	for _, child := range b.children {
		child.storage = b.storage
	}

	if err := decoder.Decode(&b.ID); err != nil {
		return err
	}
	if err := decoder.Decode(&b.T); err != nil {
		return err
	}
	if err := decoder.Decode(&b.Leaf); err != nil {
		return err
	}
	if err := decoder.Decode(&b.records); err != nil {
		return err
	}

	return nil
}

func (b *BNode) registerWrite(reason string) error {
	if b.storage == nil {
		return fmt.Errorf("Cannot register write. No storage instance")
	}
	b.storage.Write(b, reason)
	return nil
}

func (b *BNode) registerDelete(reason string) error {
	if b.storage == nil {
		return fmt.Errorf("Cannot register write. No storage instance")
	}
	b.storage.Delete(b, reason)
	return nil
}

func (b *BNode) read() error {
	return nil
	//if b.loaded {
	//return nil
	//}

	//if b.storage == nil {
	//return fmt.Errorf("Could not read node. No storage instance")
	//}

	//err := b.storage.Read(b.ID, b)
	//if err != nil {
	//return err
	//}

	//b.loaded = true
	//fmt.Println(b)

	//return nil
}

func (b *BNode) clone() *BNode {
	c := make([]*BNode, len(b.children))
	r := make([]record.Record, len(b.records))

	copy(c, b.children)
	copy(r, b.records)

	return &BNode{
		loaded: true,
		ID: b.ID,
		children: c,
		records: r,

	}
}
