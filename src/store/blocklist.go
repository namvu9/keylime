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

type Blocklist struct {
	Head      ID
	Tail      ID
	BlockSize int // Number of records inside each node
	Blocks    int // Number of blocks
	Docs      int // Number of docs

	repo repository.Repository
}

func (bl *Blocklist) GetBlock(id ID) (*Block, error) {
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

func (bl *Blocklist) insert(ctx context.Context, doc types.Document) (ref string, err error) {
	log.Printf("Block list: inserting %s in scope %s\n", doc.Key, bl.repo.Scope())
	headNode, err := bl.GetBlock(bl.Head)
	if err != nil {
		return ref, err
	}

	if headNode.Full() {
		newHead, err := bl.New()
		if err != nil {
			return ref, err
		}

		err = bl.setHeadNode(newHead)
		if err != nil {
			return ref, err
		}

		err = newHead.Insert(doc)
		if err != nil {
			return ref, err
		}

		bl.Blocks++
		return newHead.ID(), nil
	}

	err = headNode.Insert(doc)
	if err != nil {
		return ref, err
	}

	bl.Docs++
	log.Printf("Block list: done inserting %s\n", doc.Key)
	return string(bl.Head), nil
}

func (bl *Blocklist) setHeadNode(node *Block) error {
	headNode, err := bl.GetBlock(bl.Head)
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

	return nil
}

func (bl *Blocklist) remove(ctx context.Context, k string) error {
	block, _ := bl.GetBlock(bl.Head)
	for block != nil {
		for i, record := range block.Docs {
			if record.Key == k {
				block.Docs[i].Deleted = true
				bl.Docs--
				return block.save()
			}
		}

		block, _ = bl.GetBlock(block.Next)
	}

	return fmt.Errorf("Key not found: %s", k)
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

	bl.Blocks++

	log.Printf("Done creating block list in scope %s\n", bl.repo.Scope())
	return nil
}

func (bl *Blocklist) GetN(n int, asc bool) []types.Document {
	if asc {
		log.Printf("(*Blocklist).GetN: first %d\n", n)
	} else {
		log.Printf("(*Blocklist).GetN: last %d\n", n)
	}
	defer log.Println("(*Blocklist).GetN: Done")

	out := []types.Document{}

	var node *Block
	if asc {
		node, _ = bl.GetBlock(bl.Tail)
	} else {
		node, _ = bl.GetBlock(bl.Head)
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

			node, _ = bl.GetBlock(node.Prev)
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

			node, _ = bl.GetBlock(node.Next)
		}

	}

	return out
}

func (bl *Blocklist) update(ctx context.Context, r types.Document) error {
	block, _ := bl.GetBlock(bl.Head)
	for block != nil {
		for i, record := range block.Docs {
			if r.Key == record.Key {
				block.Docs[i] = r
				return block.save()
			}
		}

		block, _ = bl.GetBlock(block.Next)
	}

	return fmt.Errorf("Key not found: %s", r.Key)
}

func (bl *Blocklist) New() (*Block, error) {
	item := bl.repo.New()
	node, ok := item.(*Block)
	if !ok {
		return nil, fmt.Errorf("TODO")
	}

	return node, nil
}

func (bl *Blocklist) Info() string {
	var sb strings.Builder

	sb.WriteString("<Block list>\n")
	sb.WriteString(fmt.Sprintf("Block size: %d\n", bl.BlockSize))
	sb.WriteString(fmt.Sprintf("Blocks: %d\n", bl.Blocks))
	sb.WriteString(fmt.Sprintf("Docs: %d", bl.Docs))

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
		if doc.Key == k && !doc.Deleted {
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
