package store

import (
	"bytes"
	"encoding/gob"
	"io"
	"path"
)

type WriteBuffer struct {
	ReadWriterTo
	writeBuf  map[string]Named
	deleteBuf map[string]Named
}

type Named interface {
	Name() string
}

// Write schedules a page for being written to storage. If a
// page has already been scheduled for a write or delete,
// Write is a no-op.
func (wb *WriteBuffer) Write(p Named) error {
	if _, ok := wb.deleteBuf[p.Name()]; !ok {
		wb.writeBuf[p.Name()] = p
	}
	return nil
}

func (wb *WriteBuffer) Delete(p Named) error {
	wb.deleteBuf[p.Name()] = p
	delete(wb.writeBuf, p.Name())
	return nil
}

func (wb *WriteBuffer) flush() error {
	defer func() {
		for k := range wb.writeBuf {
			delete(wb.writeBuf, k)
		}

		for k := range wb.deleteBuf {
			delete(wb.deleteBuf, k)
		}
	}()

	for id, p := range wb.writeBuf {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)

		switch v := p.(type) {
		case *Page:
			if err := enc.Encode(v.ToSerialized()); err != nil {
				return err
			}
			_, err := wb.WithSegment(id).Write(buf.Bytes())
			if err != nil {
				return err
			}
		case *Node:
			if err := enc.Encode(v); err != nil {
				return err
			}
			_, err := wb.WithSegment(id).Write(buf.Bytes())
			if err != nil {
				return err

			}

		}
	}

	for k := range wb.deleteBuf {
		wb.WithSegment(k).Delete()
	}

	return nil
}

func newWriteBuffer(rw ReadWriterTo) *WriteBuffer {
	bs := &WriteBuffer{
		newIOReporter(),
		make(map[string]Named),
		make(map[string]Named),
	}

	if rw != nil {
		bs.ReadWriterTo = rw
	}

	return bs
}

type ioReporter struct {
	root     *ioReporter
	location string
	writes   map[string]bool
	deletes  map[string]bool
	reads    map[string]bool
}

func (ior *ioReporter) Write(src []byte) (int, error) {
	ior.root.writes[ior.location] = true

	return 0, nil
}

func (ior *ioReporter) Read(dst []byte) (int, error) {
	ior.root.reads[ior.location] = true
	return 0, nil
}

func (ior *ioReporter) Delete() error {
	ior.root.deletes[ior.location] = true
	return nil
}

func (ior *ioReporter) Exists() (bool, error) {
	return true, nil
}

func (ior *ioReporter) Open(loc string) (io.ReadWriter, error) {
	ior.location = loc
	return ior, nil
}

func (ior *ioReporter) WithSegment(s string) ReadWriterTo {
	return &ioReporter{
		root:     ior.root,
		location: path.Join(ior.location, s),
	}
}

func newIOReporter() *ioReporter {
	mrw := &ioReporter{
		writes:  make(map[string]bool),
		reads:   make(map[string]bool),
		deletes: make(map[string]bool),
	}
	mrw.root = mrw

	return mrw
}
