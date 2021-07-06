package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

// KeyIndex represents a B-tree that indexes records by key
type KeyIndex struct {
	RootPage string
	Height   int
	T        int

	root      *Page
	bufWriter *WriteBuffer
	storage   ReadWriterTo
}

func (ki *KeyIndex) insert(ctx context.Context, doc types.Document) (*types.Document, error) {
	const op errors.Op = "(*KeyIndex).Insert"

	if ki.root.full() {
		newRoot := ki.newPage(false)
		newRoot.children = []*Page{ki.root}
		newRoot.splitChild(0)
		newRoot.save()

		ki.RootPage = newRoot.ID
		ki.root = newRoot
		ki.Height++

		newRoot.save()
		ki.save()
	}

	page, err := ki.root.iter(byKey(doc.Key)).forEach(splitFullPage).Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	if idx, ok := page.keyIndex(doc.Key); ok {
		oldDoc := page.docs[idx]
		page.insert(doc)
		return &oldDoc, nil
	}

	page.insert(doc)
	return nil, nil
}

func (ki *KeyIndex) remove(ctx context.Context, key string) error {
	const op errors.Op = "(*KeyIndex).remove"

	page, err := ki.root.iter(byKey(key)).forEach(handleSparsePage).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if err := page.remove(key); err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if ki.root.empty() && !ki.root.leaf {
		oldRoot := ki.root

		newRoot, err := ki.root.child(0)
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		ki.root = newRoot
		ki.RootPage = ki.root.ID
		ki.Height--

		oldRoot.deletePage()
		newRoot.save()
		ki.save()
	}

	return ki.bufWriter.flush()
}

func (ki *KeyIndex) get(ctx context.Context, key string) (*types.Document, error) {
	const op errors.Op = "(*KeyIndex).Get"

	node, err := ki.root.iter(byKey(key)).Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	i, ok := node.keyIndex(key)
	if !ok {
		return nil, errors.NewKeyNotFoundError(op, key)
	}

	return &node.docs[i], nil
}

type KeyIndexSerialized struct {
	RootPage string
	T        int
	Height   int
}

func (ki *KeyIndex) save() error {
	var op errors.Op = "(*KeyIndex).Save"

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(ki)

	_, err := ki.storage.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	return nil
}

func (ki *KeyIndex) create() error {
	var op errors.Op = "(*KeyIndex).Create"

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(ki)

	_, err := ki.storage.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	err = ki.root.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return ki.bufWriter.flush()
}
func (ki *KeyIndex) read() error {
	const op errors.Op = "(*KeyIndex).read"

	data, err := io.ReadAll(ki.storage)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(ki)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	return nil
}

func (ki *KeyIndex) Load() error {
	var op errors.Op = "(*KeyIndex).Load"

	err := ki.read()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return ki.loadRoot()
}

func (ki *KeyIndex) loadRoot() error {
	var op errors.Op = "(*KeyIndex).loadRoot"
	ki.root = newPageWithID(ki.T, ki.RootPage, ki.bufWriter)

	err := ki.root.load()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return nil
}

func (ki *KeyIndex) Info() {
	in := Info{}
	in.validate(ki.root, true)

	fmt.Println("<KeyIndex>")
	fmt.Println("Height:", ki.Height)
	fmt.Println("T:", ki.T)
	fmt.Println("Pages:", len(in.pages))

	keys := []string{}
	for _, r := range in.records {
		keys = append(keys, r.Key)
	}

	fmt.Printf("Docs: %d\n", len(in.records))
}

type ID string
type NodeMap map[ID]*Node

// OrderIndex indexes records by their order with respect to
// some attribute
type OrderIndex struct {
	Head      ID
	Tail      ID
	BlockSize int // Number of records inside each node

	repo  repository.Repository
	nodes NodeMap
}

func (oi *OrderIndex) ID() string {
	return "order_index"
}

func (oi *OrderIndex) Node(id ID) (*Node, error) {
	if id == "" {
		return nil, fmt.Errorf("No ID provided")
	}

	item, err := oi.repo.Get(string(id))
	if err != nil {
		return nil, err
	}

	v, ok := item.(*Node)
	if !ok {
		return nil, fmt.Errorf("Item with ID %s did not have type Node", id)
	}

	v.repo = &oi.repo

	return v, nil
}

type Node struct {
	Identifier ID
	Prev       ID
	Next       ID
	Capacity   int
	Docs       []types.Document

	storage ReadWriterTo
	writer  *WriteBuffer
	repo    *repository.Repository
}

func (n *Node) ID() string {
	return string(n.Identifier)
}

func newNode(capacity int, s ReadWriterTo, w *WriteBuffer, r *repository.Repository) *Node {
	n := &Node{
		Identifier: ID(uuid.NewString()),
		Capacity:   capacity,
		storage:    newIOReporter(),
		writer:     w,
		repo:       r,
	}

	if s != nil {
		n.storage = s.WithSegment(string(n.Identifier))
	}

	return n
}

func (n *Node) Full() bool {
	return len(n.Docs) >= n.Capacity
}

func (n *Node) Insert(doc types.Document) error {
	n.Docs = append(n.Docs, doc)

	return n.save()
}

func (n *Node) save() error {
	return n.repo.Save(n)
}

func (n *Node) Name() string {
	return string(n.Identifier)
}

// TODO: Bug, document will be inserted at the head of the
// list regardless of whether it already exists
func (oi *OrderIndex) insert(ctx context.Context, doc types.Document) error {
	headNode, err := oi.Node(oi.Head)
	if err != nil {
		return err
	}

	if headNode.Full() {
		newHead := oi.New()
		err := oi.setHeadNode(newHead)
		if err != nil {
			return err
		}

		return newHead.Insert(doc)
	}

	return headNode.Insert(doc)
}

func (oi *OrderIndex) setHeadNode(node *Node) error {
	headNode, err := oi.Node(oi.Head)
	if err != nil {
		return err
	}

	headNode.Prev = node.Identifier
	node.Next = headNode.Identifier
	oi.Head = node.Identifier

	err = oi.repo.Save(headNode)
	if err != nil {
		return err
	}

	err = oi.repo.Save(node)
	if err != nil {
		return err
	}

	return oi.repo.Save(oi)
}

func (oi *OrderIndex) remove(ctx context.Context, k string) error {
	node, _ := oi.Node(oi.Head)
	for node != nil {
		for i, record := range node.Docs {
			if record.Key == k {
				node.Docs[i].Deleted = true
				return node.save()
			}
		}

		node, _ = oi.Node(node.Next)
	}

	return fmt.Errorf("Key not found: %s", k)
}

func (oi *OrderIndex) save() error {
	return oi.repo.Save(oi)
}

func (oi *OrderIndex) create() error {
	// WRAP OP
	headNode, err := oi.Node(oi.Head)
	if err != nil {
		return err
	}

	err = oi.repo.Save(headNode)
	if err != nil {
		return err
	}

	if oi.Head != oi.Tail {
		tailNode, err := oi.Node(oi.Tail)
		if err != nil {
			return err
		}

		err = oi.repo.Save(tailNode)
		if err != nil {
			return err
		}
	}

	err = oi.repo.Save(oi)
	if err != nil {
		return err
	}

	return oi.repo.Flush()
}

func (oi *OrderIndex) Get(n int, asc bool) []types.Document {
	out := []types.Document{}

	var node *Node
	if asc {
		node, _ = oi.Node(oi.Tail)
	} else {
		node, _ = oi.Node(oi.Head)
	}

	for node != nil {
		if asc {
			for _, r := range node.Docs {
				if len(out) == n {
					return out
				}
				if !r.Deleted {
					out = append(out, r)
				}
			}

			node, _ = oi.Node(node.Prev)
		} else {
			for i := len(node.Docs) - 1; i >= 0; i-- {
				if len(out) == n {
					return out
				}

				r := node.Docs[i]

				if !r.Deleted {
					out = append(out, r)
				}
			}

			node, _ = oi.Node(node.Next)
		}

	}

	return out
}

func (oi *OrderIndex) update(ctx context.Context, r types.Document) error {
	node, _ := oi.Node(oi.Head)
	for node != nil {
		for i, record := range node.Docs {
			if r.Key == record.Key {
				node.Docs[i] = r
				return node.save()
			}
		}

		node, _ = oi.Node(node.Next)
	}

	return fmt.Errorf("Key not found: %s", r.Key)
}

func (oi *OrderIndex) Info() {
	nDocs := 0
	nNodes := 0

	node, err := oi.Node(oi.Head)
	if err != nil {
		fmt.Println(err)
		return
	}
	for node != nil {
		nNodes++
		for _, r := range node.Docs {
			if !r.Deleted {
				nDocs++
			}
		}

		node, _ = oi.Node(node.Next)
	}

	fmt.Println("<OrderIndex>")
	fmt.Println("Block size:", oi.BlockSize)
	fmt.Println("Nodes:", nNodes)
	fmt.Printf("Docs: %d\n", nDocs)
}

func newOrderIndex(blockSize int, s repository.Repository) *OrderIndex {
	repo := repository.WithFactory(s, &NodeFactory{repo: s, capacity: blockSize})
	oi := &OrderIndex{
		BlockSize: blockSize,
		repo:      repo,
	}

	node := oi.New()

	oi.Head = ID(node.ID())
	oi.Tail = ID(node.ID())

	return oi
}

type NodeFactory struct {
	capacity int
	repo     repository.Repository
	writer   ReadWriterTo
}

func (nf *NodeFactory) New() types.Identifiable {
	return newNode(nf.capacity, nil, nil, &nf.repo)
}

func (nf *NodeFactory) Restore(item types.Identifiable) error {
	node, ok := item.(*Node)
	if !ok {
		return fmt.Errorf("NodeFactory does not know how to handle item %v", item)
	}

	node.repo = &nf.repo
	node.writer = newWriteBuffer(nf.writer)

	return nil
}

func (oi *OrderIndex) New() *Node {
	item := oi.repo.New()
	node := item.(*Node)

	return node
}

func newKeyIndex(t int, s ReadWriterTo) *KeyIndex {
	ki := &KeyIndex{
		T:       t,
		storage: newIOReporter(),
	}

	if s != nil {
		ki.storage = s.WithSegment("key_index")
	}

	ki.bufWriter = newWriteBuffer(s)

	ki.root = ki.newPage(true)
	ki.RootPage = ki.root.ID

	return ki
}

func (ki *KeyIndex) newPage(leaf bool) *Page {
	p := newPage(ki.T, leaf, ki.bufWriter)
	return p
}
