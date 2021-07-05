package repository

import (
	"fmt"
	"io"
	"path"

	"github.com/namvu9/keylime/src/types"
)

type Factory interface {
	New() types.Identifiable
}

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type Opener interface {
	Open(name string) (io.ReadWriter, error)
}

type Storage interface {
	Opener
}

type NoOpFactory struct{}

func (n NoOpFactory) New() types.Identifiable {
	return nil
}

type noOpIdent struct{}

func (n noOpIdent) ID() string {
	return "No-op"
}

type Repository struct {
	scope   string
	storage Storage
	codec   Codec
	factory Factory
	items   map[string]map[string]types.Identifiable

	buffer map[string]types.Identifiable
}

func (r Repository) Delete(item types.Identifiable) error {
	return nil
}

func (r Repository) Exists() (bool, error) {
	return false, nil
}

func (r Repository) Get(id string) (types.Identifiable, error) {
	return r.get(id, r.scope)
}

// New returns the object created by the repository's
// Factory.
func (r Repository) New() types.Identifiable {
	n := r.factory.New()
	r.items[r.scope][n.ID()] = n

	return n
}

func (r Repository) Flush() error {
	defer func() {
		for id := range r.buffer {
			delete(r.buffer, id)
		}
	}()

	for id, item := range r.buffer {
		items, ok := r.items[r.scope]
		if !ok {
			return fmt.Errorf("Current scope %s does not exist", r.scope)
		}

		data, err := r.codec.Encode(&item)
		if err != nil {
			return err
		}

		w, err := r.storage.Open(path.Join(r.scope, id))
		if err != nil {
			return err
		}

		_, err = w.Write(data)
		if err != nil {
			return err
		}

		items[id] = item
	}

	return nil
}

func (r Repository) Save(i types.Identifiable) error {
	if i.ID() == "" {
		return fmt.Errorf("ID must not be empty")
	}

	r.buffer[i.ID()] = i

	return nil
}

func (r *Repository) get(id string, scope string) (types.Identifiable, error) {
	items, ok := r.items[scope]
	if !ok {
		return nil, fmt.Errorf("Scope %s has not been registered", scope)
	}

	n, ok := items[id]
	if !ok {
		n, err := r.load(id)
		if err != nil {
			return nil, err
		}

		return n, nil
	}

	return n, nil
}

func (repo Repository) load(id string) (types.Identifiable, error) {
	r, err := repo.storage.Open(path.Join(repo.scope, id))
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var item types.Identifiable
	err = repo.codec.Decode(data, &item)
	if err != nil {
		return nil, err
	}

	repo.items[repo.scope][id] = item

	return item, nil
}

type Option func()

type NoOpCodec struct{}

func (noc NoOpCodec) Encode(interface{}) ([]byte, error) {
	return []byte{}, nil
}
func (noc NoOpCodec) Decode([]byte, interface{}) error {
	return nil
}

func New(scope string, c Codec, s Storage, opts ...Option) Repository {
	items := make(map[string]map[string]types.Identifiable)
	items[scope] = make(map[string]types.Identifiable)

	return Repository{
		scope:   scope,
		items:   items,
		factory: NoOpFactory{},
		codec:   c,
		storage: s,
		buffer: make(map[string]types.Identifiable),
	}
}
