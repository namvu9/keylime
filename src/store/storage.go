package store

import (
	"io"
	"path"
)

// TODO REMOVE THIS FILE

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

func (ior *ioReporter) Create(loc string) (io.ReadWriter, error) {
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
