package store

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
	RootID string
	Height int
	T      int

	repo repository.Repository
}

func (ki *Index) Root() (*Page, error) {
	item, err := ki.repo.Get(ki.RootID)
	if err != nil {
		return nil, err
	}

	page, ok := item.(*Page)
	if !ok {
		return nil, fmt.Errorf("Could not load Index root page")
	}

	return page, nil
}

func (ki *Index) insert(ctx context.Context, ref DocRef) error {
	log.Printf("Index: inserting %s\n", ref)
	const op errors.Op = "(*KeyIndex).Insert"
	root, err := ki.Root()
	if err != nil {
		return err
	}

	if root.full() {
		newRoot, err := ki.newPage(false)
		if err != nil {
			return err
		}

		fmt.Println("CREATED NEW ROOT")

		newRoot.Children = []string{root.ID()}
		newRoot.splitChild(0)
		newRoot.save()

		ki.RootID = newRoot.ID()
		ki.Height++

		newRoot.save()
		ki.save()
	}

	page, err := root.iter(byKey(ref.Key)).forEach(splitFullPage).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	page.insert(ref)
	log.Printf("Index: done inserting %s\n", ref)
	return nil
}

func (ki *Index) remove(ctx context.Context, key string) error {
	const op errors.Op = "(*KeyIndex).remove"
	root, err := ki.Root()
	if err != nil {
		return err
	}

	page, err := root.iter(byKey(key)).forEach(handleSparsePage).Get()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if err := page.remove(key); err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	if root.empty() && !root.Leaf {
		oldRoot := root

		newRoot, err := root.child(0)
		if err != nil {
			return errors.Wrap(op, errors.EInternal, err)
		}

		ki.RootID = newRoot.ID()
		ki.Height--

		oldRoot.deletePage()
		newRoot.save()
		ki.save()
	}

	return nil
}

func (ki *Index) get(ctx context.Context, key string) (*DocRef, error) {
	const op errors.Op = "(*KeyIndex).Get"
	root, err := ki.Root()
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

	return &node.Docs[i], nil
}

func newIndex(t int, r repository.Repository) Index {
	return Index{
		T:    t,
		repo: repository.WithFactory(r, PageFactory{t, r}),
	}
}

type PageFactory struct {
	t    int
	repo repository.Repository
}

func (pf PageFactory) New() types.Identifier {
	id := uuid.New().String()

	p := &Page{
		Name:   id,
		T:      pf.t,
		repo:   repository.WithFactory(pf.repo, pf),
	}

	return p
}

func (pf PageFactory) Restore(item types.Identifier) error {
	log.Println("Restoring page", item.ID(), pf.repo.Scope())
	page, ok := item.(*Page)
	if !ok {
		return fmt.Errorf("Could not restore Page")
	}

	page.repo = repository.WithFactory(pf.repo, pf)
	log.Println("Done restoring page", item.ID(), pf.repo.Scope())

	return nil
}

func (ki *Index) newPage(leaf bool) (*Page, error) {
	page, ok := ki.repo.New().(*Page)
	if !ok {
		return nil, fmt.Errorf("Index: Could not create new page")
	}

	return page, nil
}

func (ki *Index) save() error {
	return nil
}

func (i *Index) create() error {
	log.Printf("Creating index in scope %s\n", i.repo.Scope())
	//var op errors.Op = "(*KeyIndex).Create"

	rootPage := i.repo.New().(*Page)
	rootPage.Leaf = true
	i.RootID = rootPage.ID()

	return i.repo.Save(rootPage)
}

func (i *Index) New() (*Page, error) {
	item := i.repo.New()

	page, ok := item.(*Page)
	if !ok {
		return nil, fmt.Errorf("Index: Could not create page")
	}

	return page, nil
}

func (index *Index) Info() string {
	root, _ := index.Root()

	var sb strings.Builder
	in := Info{}

	in.validate(root, true)

	sb.WriteString("<Index>\n")
	sb.WriteString(fmt.Sprintf("Height: %d\n", index.Height))
	sb.WriteString(fmt.Sprintf("T: %d\n", index.T))
	sb.WriteString(fmt.Sprintf("Pages: %d\n", len(in.pages)))
	sb.WriteString(fmt.Sprintf("Docs: %d\n", len(in.docs)))

	return sb.String()
}

func (ki Index) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n-----\nKeyIndex\n-----\n")
	fmt.Fprintf(&sb, "Height:\t%d\n", ki.Height)
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
