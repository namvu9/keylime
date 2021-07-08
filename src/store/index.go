package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/types"
)

// Index represents a B-tree that indexes records by key
// TODO: Replace bufWriter, storage with repo
type Index struct {
	RootPage string
	Height   int
	T        int

	// TODO: Remove
	root *Page
}

func (ki *Index) insert(ctx context.Context, doc types.Document) (*types.Document, error) {
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

	page, err := ki.root.iter(byKey(doc.Key)).forEach(splitFullPage).Get()
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

	return nil
}

func (ki *Index) get(ctx context.Context, key string) (*types.Document, error) {
	const op errors.Op = "(*KeyIndex).Get"

	node, err := ki.root.iter(byKey(key)).Get()
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

	ki.root = ki.newPage(true)
	ki.RootPage = ki.root.ID

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
	var op errors.Op = "(*KeyIndex).loadRoot"
	ki.root = newPageWithID(ki.T, ki.RootPage)

	err := ki.root.load()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return nil
}

func (ki *Index) Info() {
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

	fmt.Printf("Docs: %d\n", len(in.records))
}

func (ki Index) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n-----\nKeyIndex\n-----\n")
	fmt.Fprintf(&sb, "Height:\t%d\n", ki.Height)
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
