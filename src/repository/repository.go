package repository

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/namvu9/keylime/src/types"
)

type Factory interface {
	New() types.Identifier

	// Inject any dependencies objects may need
	Restore(types.Identifier) error
}

type Codec interface {
	Encode(v interface{}) ([]byte, error)
	Decode(r io.Reader, v interface{}) error
}

type Opener interface {
	Open(name string) (io.ReadWriter, error)
	Delete(name string) error
}

type Storage interface {
	Opener
	Create(string) (io.ReadWriter, error)
}

type NoOpFactory struct{}

func (n NoOpFactory) New() types.Identifier {
	return nil
}

func (n NoOpFactory) Restore(types.Identifier) error {
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

	items        map[string]map[string]types.Identifier
	buffer       map[string]map[string]types.Identifier
	deleteBuffer map[string]map[string]types.Identifier
}

func (r Repository) Delete(item types.Identifier) error {
	deletes, ok := r.deleteBuffer[r.scope]
	if !ok {
		return fmt.Errorf("scope %s has not been registered", r.scope)
	}
	deletes[item.ID()] = item

	return nil
}

func (r Repository) Exists(id string) (bool, error) {
	if _, err := os.Stat(path.Join(r.scope, id)); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (r Repository) Get(id string) (types.Identifier, error) {
	items, ok := r.items[r.scope]
	if !ok {
		return nil, fmt.Errorf("Scope %s has not been registered", r.scope)
	}

	n, ok := items[id]
	if !ok {
		ok, err := r.Exists(id)
		if ok {
			log.Printf("Repository: Loading object with ID %s\n", id)
			n, err := r.load(id)
			if err != nil {
				return nil, err
			}

			return n, nil
		}
		return nil, err
	}

	return n, nil
}

// New returns the object created by the repository's
// Factory.
func (r Repository) New() types.Identifier {
	n := r.factory.New()
	r.items[r.scope][n.ID()] = n

	return n
}

func (r Repository) Flush() error {
	defer func() {
		for id, item := range r.buffer[r.scope] {
			delete(r.buffer[r.scope], id)

			r.items[r.scope][id] = item
		}

		for id := range r.deleteBuffer[r.scope] {
			delete(r.deleteBuffer[r.scope], id)
		}
	}()

	for id, item := range r.buffer[r.scope] {
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

		log.Printf("repository.Repository: wrote %s to scope %s\n", id, r.scope)
	}

	for id := range r.deleteBuffer[r.scope] {
		items, ok := r.items[r.scope]
		if !ok {
			return fmt.Errorf("Current scope %s does not exist", r.scope)
		}

		err := r.storage.Delete(path.Join(r.scope, id))
		if err != nil {
			return err
		}

		delete(items, id)

		log.Printf("repository.Repository: wrote %s to scope %s\n", id, r.scope)
	}

	return nil
}

func (r Repository) Save(i types.Identifier) error {
	if i.ID() == "" {
		return fmt.Errorf("ID must not be empty")
	}

	r.buffer[r.scope][i.ID()] = i

	return nil
}

// Saves item and commits immediately. It is equivalent to
// calling `r.Save(i)`, followed by `r.Flush()`
func (r Repository) SaveCommit(i types.Identifier) error {
	if err := r.Save(i); err != nil {
		return err
	}

	return r.Flush()
}

func (r Repository) Scope() string {
	return r.scope
}

func (repo *Repository) load(id string) (types.Identifier, error) {
	r, err := repo.storage.Open(path.Join(repo.scope, id))
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var item types.Identifier
	err = repo.codec.Decode(bytes.NewBuffer(data), &item)
	if err != nil {
		return nil, err
	}

	err = repo.factory.Restore(item)
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
func (noc NoOpCodec) Decode(io.Reader, interface{}) error {
	return nil
}

func New(scope string, c Codec, s Storage, opts ...Option) Repository {
	items := make(map[string]map[string]types.Identifier)
	items[scope] = make(map[string]types.Identifier)

	buffer := make(map[string]map[string]types.Identifier)
	buffer[scope] = make(map[string]types.Identifier)

	deleteBuffer := make(map[string]map[string]types.Identifier)
	deleteBuffer[scope] = make(map[string]types.Identifier)

	return Repository{
		scope:        scope,
		items:        items,
		factory:      NoOpFactory{},
		codec:        c,
		storage:      s,
		buffer:       buffer,
		deleteBuffer: deleteBuffer,
	}
}
