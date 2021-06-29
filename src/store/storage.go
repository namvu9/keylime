package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path"
)

type MockReadWriterTo struct {
	root     *MockReadWriterTo
	location string
	writes   map[string]bool
	deletes  map[string]bool
	reads    map[string]bool
}

func (rwt *MockReadWriterTo) Write(src []byte) (int, error) {
	rwt.root.writes[rwt.location] = true

	return 0, nil
}

func (rwt *MockReadWriterTo) Read(dst []byte) (int, error) {
	rwt.root.reads[rwt.location] = true
	return 0, nil
}

func (rwt *MockReadWriterTo) Delete() error {
	fmt.Println("DELETING", rwt.location)
	rwt.root.deletes[rwt.location] = true
	return nil
}

func (rwt *MockReadWriterTo) Exists() (bool, error) {
	return true, nil
}

func (mrwt *MockReadWriterTo) WithSegment(s string) ReadWriterTo {
	rwt := &MockReadWriterTo{
		root:     mrwt.root,
		location: path.Join(mrwt.location, s),
	}
	return rwt
}

func newMockReadWriterTo() *MockReadWriterTo {
	mrw := &MockReadWriterTo{
		writes:  make(map[string]bool),
		reads:   make(map[string]bool),
		deletes: make(map[string]bool),
	}
	mrw.root = mrw

	return mrw
}

type BufferedStorage struct {
	ReadWriterTo
	writeBuf  map[string]*Page
	deleteBuf map[string]*Page
}

// Write schedules a page for being written to disk. If a
// page has already been scheduled for a write or delete,
// Write is a no-op.
func (bs *BufferedStorage) Write(p *Page) error {
	if _, ok := bs.deleteBuf[p.ID]; !ok {
		bs.writeBuf[p.ID] = p
	}
	return nil
}

func (bs *BufferedStorage) Delete(p *Page) error {
	bs.deleteBuf[p.ID] = p
	delete(bs.writeBuf, p.ID)
	return nil
}

func (bs *BufferedStorage) flush() error {
	defer func() {
		for k := range bs.writeBuf {
			delete(bs.writeBuf, k)
		}

		for k := range bs.deleteBuf {
			delete(bs.deleteBuf, k)
		}
	}()

	for id, p := range bs.writeBuf {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)

		ps := p.ToSerialized()

		if err := enc.Encode(ps); err != nil {
			return err
		}
		_, err := bs.WithSegment(id).Write(buf.Bytes())
		if err != nil {
			return err
		}
	}

	for k := range bs.deleteBuf {
		bs.WithSegment(k).Delete()
	}

	return nil
}

func newBufferedStorage(rw ReadWriterTo) *BufferedStorage {
	bs := &BufferedStorage{
		newMockReadWriterTo(),
		make(map[string]*Page),
		make(map[string]*Page),
	}

	if rw != nil {
		bs.ReadWriterTo = rw
	}

	return bs
}
