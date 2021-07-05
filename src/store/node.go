package store

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/src/types"
)

type Node struct {
	name     ID
	Prev     ID
	Next     ID
	Capacity int
	Docs     []types.Document

	storage ReadWriterTo
	writer  *WriteBuffer
	loaded  bool
}

func (n *Node) ID() string {
	return string(n.name)
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

type PageFactory struct{}

func (pf PageFactory) Load(id string) (interface{}, error) { return nil, nil }
func (pf PageFactory) New() (interface{}, error)           { return nil, nil }
func (pf PageFactory) Save(p *Page) error                  { return nil }

func newBlock(capacity int, s ReadWriterTo, w *WriteBuffer) *Node {
	n := &Node{
		name:     ID(uuid.NewString()),
		Capacity: capacity,
		storage:  newIOReporter(),
		loaded:   true,
		writer:   w,
	}

	if s != nil {
		n.storage = s.WithSegment(string(n.name))
	}

	return n
}

func newNodeWithID(id ID, s ReadWriterTo, w *WriteBuffer) *Node {
	n := &Node{name: id, storage: newIOReporter(), writer: w}
	if s != nil {
		n.storage = s.WithSegment(string(n.name))
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
	return n.writer.Write(n)
}

func (n *Node) Name() string {
	return string(n.name)
}
