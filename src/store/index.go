package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/namvu9/keylime/src/errors"
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

func (ki *KeyIndex) insert(ctx context.Context, r types.Document) error {
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

	page, err := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	page.insert(r)

	return nil
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

	return &node.records[i], nil
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

	fmt.Printf("Records: %d\n", len(in.records))
}

type ID string
type NodeMap map[ID]*Node

// OrderIndex indexes records by their order with respect to
// some attribute
type OrderIndex struct {
	Head      ID
	Tail      ID
	BlockSize int // Number of records inside each node

	storage ReadWriterTo
	nodes   NodeMap
	writer  *WriteBuffer
}

func (oi *OrderIndex) Node(id ID) (*Node, error) {
	if id == "" {
		return nil, fmt.Errorf("No ID provided")
	}

	v, ok := oi.nodes[id]
	if !ok {
		n := newNodeWithID(id, oi.storage, oi.writer)
		err := n.Load()
		if err != nil {
			return nil, err
		}

		oi.nodes[id] = n
		return n, nil
	}

	return v, nil
}

type Node struct {
	ID   ID
	Prev ID
	Next ID

	Capacity int
	Records  []types.Document
	storage  ReadWriterTo
	writer   *WriteBuffer
	loaded   bool
}

func (n *Node) Load() error {
	data, err := io.ReadAll(n.storage)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	err = dec.Decode(n)
	if err != nil {
		return err
	}

	n.loaded = true
	return nil
}

func (oi *OrderIndex) newNode() *Node {
	oldHead, _ := oi.Node(oi.Head)

	n := newNode(oi.BlockSize, oi.storage, oi.writer)
	n.loaded = true
	n.Next = oldHead.ID
	oi.nodes[n.ID] = n

	oldHead.Prev = n.ID
	oi.Head = n.ID

	return n
}

func newNode(capacity int, s ReadWriterTo, w *WriteBuffer) *Node {
	n := &Node{
		ID:       ID(uuid.NewString()),
		Capacity: capacity,
		storage:  newIOReporter(),
		loaded:   true,
		writer:   w,
	}

	if s != nil {
		n.storage = s.WithSegment(string(n.ID))
	}

	return n
}

func newNodeWithID(id ID, s ReadWriterTo, w *WriteBuffer) *Node {
	n := &Node{ID: id, storage: newIOReporter(), writer: w}
	if s != nil {
		n.storage = s.WithSegment(string(n.ID))
	}

	return n
}

func (n *Node) Full() bool {
	return len(n.Records) >= n.Capacity
}

func (n *Node) Insert(r types.Document) error {
	n.Records = append(n.Records, r)

	return n.Save()
}

func (n *Node) Save() error {
	return n.writer.Write(n)
}

func (n *Node) Name() string {
	return string(n.ID)
}

func (oi *OrderIndex) insert(ctx context.Context, r types.Document) error {
	headNode, err := oi.Node(oi.Head)
	if err != nil {
		return err
	}
	if headNode.Full() {
		headNode = oi.newNode()
		err := oi.save()
		if err != nil {
			return err
		}
	}

	return headNode.Insert(r)
}

func (oi *OrderIndex) remove(ctx context.Context, k string) error {
	node, _ := oi.Node(oi.Head)
	for node != nil {
		for i, record := range node.Records {
			if record.Key == k {
				node.Records[i].Deleted = true
				return node.Save()
			}
		}

		node, _ = oi.Node(node.Next)
	}

	return fmt.Errorf("Key not found: %s", k)
}

func (oi *OrderIndex) save() error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(oi)
	if err != nil {
		return err
	}

	_, err = oi.storage.WithSegment("order_index").Write(buf.Bytes())
	return err
}

func (oi *OrderIndex) load() error {
	data, err := io.ReadAll(oi.storage.WithSegment("order_index"))
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	err = dec.Decode(oi)
	if err != nil {
		return err
	}

	return nil
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
			for _, r := range node.Records {
				if len(out) == n {
					return out
				}
				if !r.Deleted {
					out = append(out, r)
				}
			}

			node, _ = oi.Node(node.Prev)
		} else {
			for i := len(node.Records) - 1; i >= 0; i-- {
				if len(out) == n {
					return out
				}

				r := node.Records[i]

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
		for i, record := range node.Records {
			if r.Key == record.Key {
				node.Records[i] = r
				return node.Save()
			}
		}

		node, _ = oi.Node(node.Next)
	}

	return fmt.Errorf("Key not found: %s", r.Key)
}

func (oi *OrderIndex) Info() {
	nRecords := 0
	nNodes := 0

	node, _ := oi.Node(oi.Head)
	for node != nil {
		nNodes++
		for _, r := range node.Records {
			if !r.Deleted {
				nRecords++
			}
		}

		node, _ = oi.Node(node.Next)
	}

	fmt.Println("<OrderIndex>")
	fmt.Println("Block size:", oi.BlockSize)
	fmt.Println("Nodes:", nNodes)
	fmt.Printf("Records: %d\n", nRecords)

}

func newOrderIndex(blockSize int, s ReadWriterTo) *OrderIndex {
	wb := newWriteBuffer(s)
	node := newNode(blockSize, s, wb)
	oi := &OrderIndex{
		BlockSize: blockSize,
		storage:   newIOReporter(),
		Head:      node.ID,
		Tail:      node.ID,
		nodes:     NodeMap{node.ID: node},
		writer:    wb,
	}

	if s != nil {
		oi.storage = s
	}

	return oi
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

