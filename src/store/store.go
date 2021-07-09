package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

type Store struct {
	baseDir string
	t       int

	repo repository.Repository
}

type CollectionFactory struct {
	repo repository.Repository
}

func (cf CollectionFactory) New() types.Identifier {
	nop := repository.NoOpFactory{}
	return nop.New()
}

func (cf CollectionFactory) Restore(item types.Identifier) error {
	c, ok := item.(*Collection)
	if !ok {
		return fmt.Errorf("CollectionFactory does not know how to handle item %v", item)
	}

	c.repo = repository.WithScope(cf.repo, c.ID())

	return c.load()
}

func newCollectionFactory(r repository.Repository) CollectionFactory {
	return CollectionFactory{
		repo: r,
	}
}

func (s *Store) Collection(name string) (types.Collection, error) {
	repo := repository.WithScope(s.repo, name)

	if ok, err := repo.Exists(name); !ok && err == nil {
		c := newCollection(name, s.repo)
		return c, nil
	} else if err != nil {
		return nil, err
	}

	item, err := repo.Get(name)
	if err != nil {
		return nil, err
	}

	c, ok := item.(*Collection)
	if !ok {
		return nil, fmt.Errorf("Could not load collection %s", name)
	}

	return c, nil
}

type DefaultCodec struct{}

func (dc DefaultCodec) Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}

	b := buf.Bytes()
	return b, nil
}

func (dc DefaultCodec) Decode(r io.Reader, dst interface{}) error {
	dec := gob.NewDecoder(r)
	err := dec.Decode(dst)
	if err != nil {
		return err
	}

	return nil
}

// New instantiates a store with the provided config and
// options
func New(cfg *Config, opts ...Option) *Store {
	s := &Store{
		baseDir: cfg.BaseDir,
		t:       cfg.T,
		repo:    repository.New(cfg.BaseDir, DefaultCodec{}, repository.NewFS(cfg.BaseDir)),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.repo = repository.WithFactory(s.repo, newCollectionFactory(s.repo))

	return s
}
