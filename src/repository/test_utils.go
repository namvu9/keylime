package repository

import "io"

type IOReporter struct {
	root     *IOReporter
	location string
	Writes   map[string]bool
	Deletes  map[string]bool
	Reads    map[string]bool
}

func (ior *IOReporter) Write(src []byte) (int, error) {
	ior.root.Writes[ior.location] = true

	return 0, nil
}

func (ior *IOReporter) Read(dst []byte) (int, error) {
	ior.root.Reads[ior.location] = true
	return 0, nil
}

func (ior *IOReporter) Delete(loc string) error {
	ior.Deletes[loc] = true
	return nil
}

func (ior *IOReporter) Exists() (bool, error) {
	return true, nil
}

func (ior *IOReporter) Open(loc string) (io.ReadWriter, error) {
	ior.location = loc
	return ior, nil
}

func (ior *IOReporter) Create(loc string) (io.ReadWriter, error) {
	ior.location = loc
	return ior, nil
}

func newIOReporter() *IOReporter {
	mrw := &IOReporter{
		Writes:  make(map[string]bool),
		Reads:   make(map[string]bool),
		Deletes: make(map[string]bool),
	}
	mrw.root = mrw

	return mrw
}
