package storage

import (
	"encoding/gob"
	"fmt"
	"io"
	"path"
)

type Factory interface {
	New() (Identifiable, error)
}

type Identifiable interface {
	ID() string
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

// Loader manages blablalba
// We can use BlockManager with anything that contains an ID
type BlockManager struct {
	basePath  string
	storage   Storage
	codec     Codec
	factories map[string]Factory
	items     map[string]map[string]Identifiable
}

func (bm *BlockManager) RegisterTypes(prototypes ...interface{}) {
	for _, p := range prototypes {
		gob.Register(p)
	}
}

func (bm *BlockManager) RegisterFactory(scope string, f Factory) {
	bm.factories[scope] = f
	bm.items[scope] = make(map[string]Identifiable)
}

func (bm *BlockManager) Scope(name string) *ScopedBlockManager {
	return &ScopedBlockManager{
		bm,
		name,
	}
}

type ScopedBlockManager struct {
	bm    *BlockManager
	scope string
}

func (sbm *ScopedBlockManager) Scope(name string, f Factory) *ScopedBlockManager {
	return sbm.bm.Scope(path.Join(sbm.scope, name))
}

func (sbm ScopedBlockManager) Delete(item Identifiable) error {
	return nil
}

// Create the current scope
func (sbm ScopedBlockManager) Create() error {
  return nil
}

func (sbm ScopedBlockManager) Exists() (bool, error) {
	return false, nil
}

func (sbm ScopedBlockManager) Get(id string) (Identifiable, error) {
	return sbm.bm.get(id, sbm.scope)
}

func (sbm ScopedBlockManager) New() (Identifiable, error) {
	return sbm.bm.create(sbm.scope)
}

func (sbm ScopedBlockManager) Save(item Identifiable) error {
	return sbm.bm.save(sbm.scope, item)
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		items:     make(map[string]map[string]Identifiable),
		factories: make(map[string]Factory),
	}
}

func (bm *BlockManager) get(id string, scope string) (Identifiable, error) {
	items, ok := bm.items[scope]
	if !ok {
		return nil, fmt.Errorf("Scope %s has not been registered", scope)
	}

	n, ok := items[id]
	if !ok {
		n, err := bm.load(scope, id)
		if err != nil {
			return nil, err
		}

		return n, nil
	}

	return n, nil
}

func (bm *BlockManager) load(scope string, id string) (Identifiable, error) {
	r, err := bm.storage.Open(path.Join(bm.basePath, scope, id))

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var item Identifiable
	err = bm.codec.Decode(data, item)
	if err != nil {
		return nil, err
	}

	bm.items[scope][id] = item

	return item, nil
}

func (bm *BlockManager) create(scope string) (Identifiable, error) {
	factory, ok := bm.factories[scope]
	if !ok {
		return nil, fmt.Errorf("Could not find factory for scope %s", scope)
	}

	n, err := factory.New()
	if err != nil {
		return nil, err
	}
	bm.items[scope][n.ID()] = n
	return n, nil
}

func (bm *BlockManager) save(scope string, i Identifiable) error {
	id := i.ID()
	items, ok := bm.items[scope]
	if !ok {
		return fmt.Errorf("No s")
	}

	item, ok := items[id]
	if !ok {
		return fmt.Errorf("No item with ID %s exists in scope %s", id, scope)
	}

	data, err := bm.codec.Encode(item)
	if err != nil {
		return err
	}

	w, err := bm.storage.Open(path.Join(bm.basePath, scope, i.ID()))
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}
