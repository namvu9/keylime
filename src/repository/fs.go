package repository

import (
	"io"
	"os"
	"path"
)

type FStorage struct {
	location string
	offset   int64
}

func (fs *FStorage) Delete(path string) error {
	return os.Remove(path)
}

func (fs *FStorage) Read(dst []byte) (int, error) {
	f, err := os.Open(fs.location)
	defer f.Close()
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(dst, int64(fs.offset))
	fs.offset += int64(n)

	if err == io.EOF {
		fs.offset = 0
	}

	return n, err
}

func (fs *FStorage) Open(location string) (io.ReadWriter, error) {
	err := os.MkdirAll(path.Dir(location), 0777)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(location, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	return &File{location, f}, nil
}

func (fs *FStorage) Create(location string) (io.ReadWriter, error) {
	return os.Create(location)
}

func (fs *FStorage) Exists() (bool, error) {
	if _, err := os.Stat(fs.location); os.IsNotExist(err) {
		return false, nil
	} else {
		return true, err
	}
}

type File struct {
	location string
	*os.File
}

func (f *File) Write(data []byte) (int, error) {
	return f.File.Write(data)
}

func NewFS(baseDir string) *FStorage {
	return &FStorage{
		location: baseDir,
		offset:   0,
	}
}


func NewMockRepo() (Repository, *IOReporter) {
	reporter := newIOReporter()
	return New("", NoOpCodec{}, reporter), reporter
}

