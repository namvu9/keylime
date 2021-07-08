package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

type ID string

// Blocklist indexes records by their order with respect to
// some attribute
type Blocklist struct {
	Head      ID
	Tail      ID
	BlockSize int // Number of records inside each node

	repo repository.Repository
}

func (oi *Blocklist) ID() string {
	return "order_index"
}

func (oi *Blocklist) Block(id ID) (*Block, error) {
	if id == "" {
		return nil, fmt.Errorf("No ID provided")
	}

	item, err := oi.repo.Get(string(id))
	if err != nil {
		return nil, err
	}

	v, ok := item.(*Block)
	if !ok {
		return nil, fmt.Errorf("Item with ID %s did not have type Node", id)
	}

	v.repo = &oi.repo

	return v, nil
}

// TODO: Bug, document will be inserted at the head of the
// list regardless of whether it already exists
func (oi *Blocklist) insert(ctx context.Context, doc types.Document) error {
	headNode, err := oi.Block(oi.Head)
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

func (oi *Blocklist) setHeadNode(node *Block) error {
	headNode, err := oi.Block(oi.Head)
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

func (oi *Blocklist) remove(ctx context.Context, k string) error {
	node, _ := oi.Block(oi.Head)
	for node != nil {
		for i, record := range node.Docs {
			if record.Key == k {
				node.Docs[i].Deleted = true
				return node.save()
			}
		}

		node, _ = oi.Block(node.Next)
	}

	return fmt.Errorf("Key not found: %s", k)
}

func (oi *Blocklist) save() error {
	return oi.repo.Save(oi)
}

func (oi *Blocklist) create() error {
	// WRAP OP
	headNode, err := oi.Block(oi.Head)
	if err != nil {
		return err
	}

	err = oi.repo.Save(headNode)
	if err != nil {
		return err
	}

	if oi.Head != oi.Tail {
		tailNode, err := oi.Block(oi.Tail)
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

func (oi *Blocklist) Get(n int, asc bool) []types.Document {
	out := []types.Document{}

	var node *Block
	if asc {
		node, _ = oi.Block(oi.Tail)
	} else {
		node, _ = oi.Block(oi.Head)
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

			node, _ = oi.Block(node.Prev)
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

			node, _ = oi.Block(node.Next)
		}

	}

	return out
}

func (oi *Blocklist) update(ctx context.Context, r types.Document) error {
	node, _ := oi.Block(oi.Head)
	for node != nil {
		for i, record := range node.Docs {
			if r.Key == record.Key {
				node.Docs[i] = r
				return node.save()
			}
		}

		node, _ = oi.Block(node.Next)
	}

	return fmt.Errorf("Key not found: %s", r.Key)
}

func (oi *Blocklist) New() *Block {
	item := oi.repo.New()
	node := item.(*Block)

	return node
}

func (oi *Blocklist) Info() {
	nDocs := 0
	nNodes := 0

	node, err := oi.Block(oi.Head)
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

		node, _ = oi.Block(node.Next)
	}

	fmt.Println("<OrderIndex>")
	fmt.Println("Block size:", oi.BlockSize)
	fmt.Println("Nodes:", nNodes)
	fmt.Printf("Docs: %d\n", nDocs)
}

func newOrderIndex(blockSize int, s repository.Repository) Blocklist {
	repo := repository.WithFactory(s, &NodeFactory{repo: s, capacity: blockSize})
	oi := Blocklist{
		BlockSize: blockSize,
		repo:      repo,
	}

	node := oi.New()

	oi.Head = ID(node.ID())
	oi.Tail = ID(node.ID())

	return oi
}

type Block struct {
	Identifier ID
	Prev       ID
	Next       ID
	Capacity   int
	Docs       []types.Document

	repo *repository.Repository
}

func (n *Block) ID() string {
	return string(n.Identifier)
}

func newNode(capacity int, r *repository.Repository) *Block {
	n := &Block{
		Identifier: ID(uuid.NewString()),
		Capacity:   capacity,
		repo:       r,
	}

	return n
}

func (n *Block) Full() bool {
	return len(n.Docs) >= n.Capacity
}

func (n *Block) Insert(doc types.Document) error {
	n.Docs = append(n.Docs, doc)

	return n.save()
}

func (n *Block) save() error {
	return n.repo.Save(n)
}

func (n *Block) Name() string {
	return string(n.Identifier)
}

type NodeFactory struct {
	capacity int
	repo     repository.Repository
}

func (nf *NodeFactory) New() types.Identifier {
	return newNode(nf.capacity, &nf.repo)
}

func (nf *NodeFactory) Restore(item types.Identifier) error {
	node, ok := item.(*Block)
	if !ok {
		return fmt.Errorf("NodeFactory does not know how to handle item %v", item)
	}

	node.repo = &nf.repo

	return nil
}
