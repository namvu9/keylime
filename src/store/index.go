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
	record "github.com/namvu9/keylime/src/types"
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

func (ki *KeyIndex) Insert(ctx context.Context, r record.Record) error {
	const op errors.Op = "(*KeyIndex).Insert"

	if ki.root.Full() {
		newRoot := ki.newPage(false)
		newRoot.children = []*Page{ki.root}
		newRoot.splitChild(0)
		newRoot.save()

		ki.RootPage = newRoot.ID
		ki.root = newRoot
		ki.Height++

		newRoot.save()
		ki.Save()
	}

	page, err := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	page.insert(r)

	return ki.bufWriter.Flush()
}

func (ki *KeyIndex) Delete(ctx context.Context, key string) error {
	const op errors.Op = "(*KeyIndex).Delete"

	page, err := ki.root.iter(byKey(key)).forEach(handleSparsePage).Get()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	if err := page.Delete(key); err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	if ki.root.Empty() && !ki.root.Leaf() {
		oldRoot := ki.root

		newRoot, err := ki.root.Child(0)
		if err != nil {
			return errors.Wrap(op, errors.InternalError, err)
		}

		ki.root = newRoot
		ki.RootPage = ki.root.ID
		ki.Height--

		oldRoot.deletePage()
		newRoot.save()
		ki.Save()
	}

	return ki.bufWriter.Flush()
}

func (ki *KeyIndex) Get(ctx context.Context, key string) (*record.Record, error) {
	const op errors.Op = "(*KeyIndex).Get"

	node, err := ki.root.iter(byKey(key)).Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.InternalError, err)
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

func (ki *KeyIndex) Save() error {
	var op errors.Op = "(*KeyIndex).Save"

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(ki)

	_, err := ki.storage.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}

	return nil
}

func (ki *KeyIndex) Create() error {
	var op errors.Op = "(*KeyIndex).Create"

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(ki)

	_, err := ki.storage.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}

	err = ki.root.save()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	return ki.bufWriter.Flush()
}
func (ki *KeyIndex) read() error {
	const op errors.Op = "(*KeyIndex).read"

	data, err := io.ReadAll(ki.storage)
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(ki)
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}

	return nil
}

func (ki *KeyIndex) Load() error {
	var op errors.Op = "(*KeyIndex).Load"

	err := ki.read()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	return ki.loadRoot()
}

func (ki *KeyIndex) loadRoot() error {
	var op errors.Op = "(*KeyIndex).loadRoot"
	ki.root = newPageWithID(ki.T, ki.RootPage, ki.bufWriter)

	err := ki.root.load()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
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
	fmt.Printf("Records (%d): %v\n", len(in.records), in.records)
}

type ID string
type NodeMap map[ID]*Node

// OrderIndex indexes records by their order with respect to
// some attribute
type OrderIndex struct {
	head ID
	tail ID

	nodes     NodeMap
	BlockSize int // Number of records inside each node
	Storage   ReadWriterTo
}

func (oi *OrderIndex) Node(id ID) (*Node, error) {
	if id == "" {
		return nil, fmt.Errorf("No ID provided")
	}

	v, ok := oi.nodes[id]
	if !ok {
		n := newNodeWithID(id, oi.Storage)
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
	Records  []*record.Record
	Storage  ReadWriterTo
	loaded   bool
}

func (n *Node) Load() error {
	n.loaded = true
	return nil
}

func (oi *OrderIndex) newNode() *Node {
	oldHead, _ := oi.Node(oi.head)

	n := newNode(oi.BlockSize, oi.Storage)
	n.loaded = true
	n.Next = oldHead.ID
	oi.nodes[n.ID] = n

	oldHead.Prev = n.ID
	oi.head = n.ID

	return n
}

func newNode(capacity int, s ReadWriterTo) *Node {
	n := &Node{ID: ID(uuid.NewString()), Capacity: capacity, Storage: newIOReporter(), loaded: true}

	if s != nil {
		n.Storage = s.WithSegment(string(n.ID))
	}

	return n
}

func newNodeWithID(id ID, s ReadWriterTo) *Node {
	n := &Node{ID: id, Storage: newIOReporter()}
	if s != nil {
		n.Storage = s.WithSegment(string(n.ID))
	}

	return n
}

func (n *Node) Full() bool {
	return len(n.Records) >= n.Capacity
}

func (n *Node) Insert(r *record.Record) error {
	n.Records = append(n.Records, r)
	return nil
}

func (oi *OrderIndex) Insert(r *record.Record) error {
	headNode, _ := oi.Node(oi.head)
	if headNode.Full() {
		headNode = oi.newNode()
	}

	return headNode.Insert(r)
}

func (oi *OrderIndex) Delete(r *record.Record) error {
	return nil
}

func (oi *OrderIndex) Get(n int, asc bool) []*types.Record {
	out := []*types.Record{}

	var node *Node
	if asc {
		node, _ = oi.Node(oi.tail)
	} else {
		node, _ = oi.Node(oi.head)
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
				r := node.Records[i]

				if len(out) == n {
					return out
				}

				if !r.Deleted {
					out = append(out, r)
				}
			}

			node, _ = oi.Node(node.Next)
		}

	}

	return out
}

func newOrderIndex(blockSize int, s ReadWriterTo) *OrderIndex {
	node := newNode(blockSize, s)
	oi := &OrderIndex{
		BlockSize: blockSize,
		Storage:   newIOReporter(),
		head:      node.ID,
		tail:      node.ID,
		nodes:     NodeMap{node.ID: node},
	}

	if s != nil {
		oi.Storage = s
	}

	return oi
}
