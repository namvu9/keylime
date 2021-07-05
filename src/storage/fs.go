package storage

import (
	"io"
	"os"
	"path"
)

type FStorage struct {
	location string
	offset   int64
}

func (fs *FStorage) Delete() error { return nil }

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

func (fs *FStorage) Open(location string) (io.ReadWriteCloser, error) {
	f, err := os.OpenFile(location, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0655)
	if err != nil {
		return nil, err
	}

	return &File{location, f}, nil
}

func (fs *FStorage) Write(src []byte) (int, error) {
	if src == nil {
		return 0, os.MkdirAll(fs.location, 0755)
	}

	return 0, os.WriteFile(fs.location, src, 0755)
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
	err := os.MkdirAll(path.Dir(f.location), 0655)
	if err != nil {
		return 0, err
	}

	return f.File.Write(data)
}

func New(baseDir string) *FStorage {
	return &FStorage{
		location: baseDir,
		offset:   0,
	}
}
