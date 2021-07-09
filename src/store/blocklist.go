package store

import (
	"context"
	"fmt"
	"log"
	"strings"

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

func (*Blocklist) ID() string {
	return "order_index"
}

func (bl *Blocklist) Get(id ID) (*Block, error) {
	if id == "" {
		return nil, fmt.Errorf("No ID provided")
	}

	item, err := bl.repo.Get(string(id))
	if err != nil {
		return nil, err
	}

	v, ok := item.(*Block)
	if !ok {
		return nil, fmt.Errorf("Item with ID %s did not have type Node", id)
	}

	v.repo = &bl.repo

	return v, nil
}

// TODO: Bug, document will be inserted at the head of the
// list regardless of whether it already exists
func (bl *Blocklist) insert(ctx context.Context, doc types.Document) (*DocRef, error) {
	log.Printf("Block list: inserting %s in scope %s\n", doc.Key, bl.repo.Scope())
	headNode, err := bl.Get(bl.Head)
	if err != nil {
		return nil, err
	}

	if headNode.Full() {
		fmt.Println("FULL")
		newHead, err := bl.New()
		if err != nil {
			return nil, err
		}

		err = bl.setHeadNode(newHead)
		if err != nil {
			return nil, err
		}

		err = newHead.Insert(doc)
		if err != nil {
			return nil, err
		}

		return &DocRef{
			Key:     doc.Key,
			BlockID: ID(newHead.ID()),
		}, nil
	}

	err = headNode.Insert(doc)
	if err != nil {
		return nil, err
	}

	log.Printf("Block list: done inserting %s\n", doc.Key)
	return &DocRef{
		Key:     doc.Key,
		BlockID: bl.Head,
	}, nil
}

func (bl *Blocklist) setHeadNode(node *Block) error {
	headNode, err := bl.Get(bl.Head)
	if err != nil {
		return err
	}

	headNode.Prev = node.Identifier
	node.Next = headNode.Identifier
	bl.Head = node.Identifier

	err = bl.repo.Save(headNode)
	if err != nil {
		return err
	}

	err = bl.repo.Save(node)
	if err != nil {
		return err
	}

	return bl.repo.Save(bl)
}

func (bl *Blocklist) remove(ctx context.Context, k string) error {
	node, _ := bl.Get(bl.Head)
	for node != nil {
		for i, record := range node.Docs {
			if record.Key == k {
				node.Docs[i].Deleted = true
				return node.save()
			}
		}

		node, _ = bl.Get(node.Next)
	}

	return fmt.Errorf("Key not found: %s", k)
}

func (bl *Blocklist) save() error {
	return bl.repo.Save(bl)
}

func (bl *Blocklist) create() error {
	log.Printf("Creating block list in scope %s\n", bl.repo.Scope())
	block, err := bl.New()
	if err != nil {
		return err
	}

	err = bl.repo.Save(block)
	if err != nil {
		return err
	}

	bl.Head = block.Identifier
	bl.Tail = block.Identifier

	log.Printf("Done creating block list in scope %s\n", bl.repo.Scope())
	return nil
}

func (bl *Blocklist) GetN(n int, asc bool) []types.Document {
	out := []types.Document{}

	var node *Block
	if asc {
		node, _ = bl.Get(bl.Tail)
	} else {
		node, _ = bl.Get(bl.Head)
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

			node, _ = bl.Get(node.Prev)
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

			node, _ = bl.Get(node.Next)
		}

	}

	return out
}

func (bl *Blocklist) update(ctx context.Context, r types.Document) error {
	node, _ := bl.Get(bl.Head)
	for node != nil {
		for i, record := range node.Docs {
			if r.Key == record.Key {
				node.Docs[i] = r
				return node.save()
			}
		}

		node, _ = bl.Get(node.Next)
	}

	return fmt.Errorf("Key not found: %s", r.Key)
}

func (bl *Blocklist) New() (*Block, error){
	item := bl.repo.New()
	node, ok := item.(*Block)
	if !ok {
		return nil, fmt.Errorf("TODO")
	}

	return node, nil
}

func (bl *Blocklist) Info() string {
	var sb strings.Builder
	nDocs := 0
	nNodes := 0

	node, err := bl.Get(bl.Head)
	if err != nil {
		return ""
	}
	for node != nil {
		nNodes++
		for _, r := range node.Docs {
			if !r.Deleted {
				nDocs++
			}
		}

		node, _ = bl.Get(node.Next)
	}

	sb.WriteString("<Block list>\n")
	sb.WriteString(fmt.Sprintf("Block size: %d\n", bl.BlockSize))
	sb.WriteString(fmt.Sprintf("Nodes: %d\n", nNodes))
	sb.WriteString(fmt.Sprintf("Docs: %d\n", nDocs))
	return sb.String()
}

func newBlocklist(blockSize int, s repository.Repository) Blocklist {
	repo := repository.WithFactory(s, &BlockFactory{repo: s, capacity: blockSize})
	bl := Blocklist{
		BlockSize: blockSize,
		repo:      repo,
	}

	return bl
}

type Block struct {
	Identifier ID
	Prev       ID
	Next       ID
	Capacity   int
	Docs       []types.Document

	repo *repository.Repository
}

func (b *Block) Get(k string) (*types.Document, error) {
	for _, doc := range b.Docs {
		if doc.Key == k {
			return &doc, nil
		}
	}

	return nil, fmt.Errorf("Key not found")
}

func (b *Block) ID() string {
	return string(b.Identifier)
}

func (b *Block) Full() bool {
	return len(b.Docs) >= b.Capacity
}

func (b *Block) Insert(doc types.Document) error {
	b.Docs = append(b.Docs, doc)

	return b.save()
}

// TODO: TEST
func (b *Block) Update(targetDoc types.Document) error {
	for i, doc := range b.Docs {
		if doc.Key == targetDoc.Key {
			b.Docs[i] = targetDoc
			return b.save()
		}
	}

	return fmt.Errorf("Key not found")
}

func (b *Block) save() error {
	return b.repo.Save(b)
}

type BlockFactory struct {
	capacity int
	repo     repository.Repository
}

func (bf *BlockFactory) New() types.Identifier {
	n := &Block{
		Identifier: ID(uuid.NewString()),
		Capacity:   bf.capacity,
		repo:       &bf.repo,
	}

	return n
}

func (bf *BlockFactory) Restore(item types.Identifier) error {
	node, ok := item.(*Block)
	if !ok {
		return fmt.Errorf("BlockFactory does not know how to handle item %v", item)
	}

	node.repo = &bf.repo

	return nil
}
