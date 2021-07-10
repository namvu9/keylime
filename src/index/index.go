package index

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

// Index represents a B-tree that indexes records by key
type Index struct {
	RootID  string
	Height  int
	Records int // Number of records in the tree
	T       int

	repo repository.Repository
}

func (index *Index) SetRepo(r repository.Repository) {
	index.repo = repository.WithFactory(r, NodeFactory{t: 200, repo: r})
}

func (index *Index) Insert(ctx context.Context, key string, value string, hash string) error {
	return index.insert(ctx, Record{key, value, hash})
}

func (index *Index) insert(ctx context.Context, ref Record) error {
	log.Printf("Index: inserting %s\n", ref)
	const op errors.Op = "(*KeyIndex).Insert"
	root, err := index.root()
	if err != nil {
		return err
	}

	if root.full() {
		newRoot, err := index.New(false)
		if err != nil {
			return err
		}

		newRoot.Children = []string{root.ID()}
		newRoot.splitChild(0)
		newRoot.save()

		index.RootID = newRoot.ID()
		index.Height++

		newRoot.save()
	}

	root, err = index.root()
	if err != nil {
		return err
	}

	node, err := root.iter(byKey(ref.Key)).forEach(splitFullNode).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = node.insert(ref)
	if err != nil {
		return err
	}

	index.Records++

	log.Printf("Index: done inserting %s\n", ref)
	return nil
}

func (index *Index) Delete(ctx context.Context, key string) error {
	const op errors.Op = "(*KeyIndex).remove"

	root, err := index.root()
	if err != nil {
		return err
	}

	node, err := root.iter(byKey(key)).forEach(handleSparseNode).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if err := node.remove(key); err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if root.empty() && !root.Leaf {
		oldRoot := root

		newRoot, err := root.child(0)
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		index.RootID = newRoot.ID()
		index.Height--

		oldRoot.deleteNode()
		newRoot.save()
	}
	index.Records--

	return nil
}

func (index *Index) Get(ctx context.Context, key string) (*Record, error) {
	const op errors.Op = "(*KeyIndex).Get"
	root, err := index.root()
	if err != nil {
		return nil, err
	}

	node, err := root.iter(byKey(key)).Get()
	if err != nil {
		return nil, errors.Wrap(op, errors.EInternal, err)
	}

	i, ok := node.keyIndex(key)
	if !ok {
		return nil, errors.NewKeyNotFoundError(op, key)
	}

	return &node.Records[i], nil
}

func (index *Index) Create() error {
	log.Printf("Creating index in scope %s\n", index.repo.Scope())

	rootNode := index.repo.New().(*Node)
	rootNode.Leaf = true
	index.RootID = rootNode.ID()

	return index.repo.Save(rootNode)
}

func (index *Index) New(leaf bool) (*Node, error) {
	item := index.repo.New()

	node, ok := item.(*Node)
	if !ok {
		return nil, fmt.Errorf("Index: Could not create node")
	}

	node.Leaf = leaf

	return node, nil
}

func (index *Index) Info() string {
	var sb strings.Builder

	sb.WriteString("<Index>\n")
	sb.WriteString(fmt.Sprintf("Height: %d\n", index.Height))
	sb.WriteString(fmt.Sprintf("T: %d\n", index.T))
	sb.WriteString(fmt.Sprintf("Docs: %d\n", index.Records))

	return sb.String()
}

func (index Index) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n-----\nKeyIndex\n-----\n")
	fmt.Fprintf(&sb, "Height:\t%d\n", index.Height)
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}

func (index *Index) root() (*Node, error) {
	item, err := index.repo.Get(index.RootID)
	if err != nil {
		return nil, err
	}

	node, ok := item.(*Node)
	if !ok {
		return nil, fmt.Errorf("Could not load Index root node")
	}

	return node, nil
}

func New(t int, r repository.Repository) Index {
	return Index{
		T:    t,
		repo: repository.WithFactory(r, NodeFactory{t, r}),
	}
}

type NodeFactory struct {
	t    int
	repo repository.Repository
}

func (pf NodeFactory) New() types.Identifier {
	id := uuid.New().String()

	p := &Node{
		Name: id,
		T:    pf.t,
		repo: repository.WithFactory(pf.repo, pf),
	}

	return p
}

func (pf NodeFactory) Restore(item types.Identifier) error {
	log.Println("Restoring node", item.ID(), pf.repo.Scope())
	node, ok := item.(*Node)
	if !ok {
		return fmt.Errorf("Could not restore Node")
	}

	node.repo = repository.WithFactory(pf.repo, pf)
	log.Println("Done restoring node", item.ID(), pf.repo.Scope())

	return nil
}
