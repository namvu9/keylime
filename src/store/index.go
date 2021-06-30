package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/namvu9/keylime/src/errors"
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

// OrderIndex indexes records by their order with respect to
// some attribute
type OrderIndex struct {
	head *Node
	tail *Node
}

type Node struct {
	ID        string
	BlockSize int
	Records   []record.Record
	prev      *Node
	next      *Node
}

func newNode() *Node {
	return &Node{}
}

func (n *Node) Full() bool {
	return len(n.Records) >= n.BlockSize
}

func (n *Node) Insert(r record.Record) error {
	n.Records = append(n.Records, r)
	return nil
}

func (oi *OrderIndex) Insert(r record.Record) error {
	if oi.head.Full() {
		n := newNode()
		n.next = oi.head
		oi.head.prev = n
		oi.head = n
		return n.Insert(r)
	}

	return oi.head.Insert(r)
}

func (oi *OrderIndex) Delete(r *record.Record) error {
	return nil
}
