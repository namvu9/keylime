package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

// Index represents a B-tree that indexes records by key
// TODO: Replace bufWriter, storage with repo
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

func (ki *Index) insert(ctx context.Context, doc types.Document) (*types.Document, error) {
	const op errors.Op = "(*KeyIndex).Insert"
	root, err := ki.Root()
	if err != nil {
		return nil, err
	}

	if root.full() {
		newRoot := ki.newPage(false)
		newRoot.children = []*Page{root}
		newRoot.splitChild(0)
		newRoot.save()

		ki.RootID = newRoot.Name
		ki.Height++

		newRoot.save()
		ki.save()
	}

	page, err := root.iter(byKey(doc.Key)).forEach(splitFullPage).Get()
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

	if root.empty() && !root.leaf {
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

func (ki *Index) get(ctx context.Context, key string) (*types.Document, error) {
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

	return &node.docs[i], nil
}

func newIndex(t int) *Index {
	ki := &Index{
		T: t,
	}

	root := ki.repo.New()
	ki.RootID = root.ID()

	return ki
}

func (ki *Index) newPage(leaf bool) *Page {
	p := newPage(ki.T, leaf)
	return p
}

func (ki *Index) save() error {
	//var op errors.Op = "(*KeyIndex).Save"

	//buf := new(bytes.Buffer)
	//enc := gob.NewEncoder(buf)
	//enc.Encode(ki)

	//_, err := ki.storage.Write(buf.Bytes())
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	return nil
}

func (ki *Index) create() error {
	//var op errors.Op = "(*KeyIndex).Create"

	//buf := new(bytes.Buffer)
	//enc := gob.NewEncoder(buf)
	//enc.Encode(ki)

	//_, err := ki.storage.Write(buf.Bytes())
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	//err = ki.root.save()
	//if err != nil {
	//return errors.Wrap(op, errors.EInternal, err)
	//}

	//return ki.bufWriter.flush()
	return nil
}
func (ki *Index) read() error {
	//const op errors.Op = "(*KeyIndex).read"

	//data, err := io.ReadAll(ki.storage)
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	//dec := gob.NewDecoder(bytes.NewBuffer(data))
	//err = dec.Decode(ki)
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	return nil
}

func (ki *Index) Load() error {
	var op errors.Op = "(*KeyIndex).Load"

	err := ki.read()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return ki.loadRoot()
}

func (ki *Index) loadRoot() error {
	//var op errors.Op = "(*KeyIndex).loadRoot"
	//ki.root = newPageWithID(ki.T, ki.RootID)

	//err := ki.root.load()
	//if err != nil {
	//return errors.Wrap(op, errors.EInternal, err)
	//}

	return nil
}

func (ki *Index) Info() {
	in := Info{}
	//in.validate(ki.root, true)

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

func (ki Index) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n-----\nKeyIndex\n-----\n")
	fmt.Fprintf(&sb, "Height:\t%d\n", ki.Height)
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
