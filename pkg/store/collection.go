package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

// A Collection is a named container for a group of records
type Collection struct {
	RootPage string
	Name     string
	T        int
	root     *Page
	baseDir  string
	s        *Store

	writeBuf map[*Page]bool
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(k string) []byte {
	node := c.root.iter(byKey(k)).Get()
	i, ok := node.keyIndex(k)
	if !ok {
		return nil
	}

	return node.records[i].Value
}

func (c *Collection) setRoot(p *Page) {
	c.root = p
	c.RootPage = p.ID
}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(k string, value []byte) error {
	if c.root.Full() {
		s := c.newPage()
		s.children = []*Page{c.root}
		s.splitChild(0)
		c.setRoot(s)

		s.save()
		c.save()
	}

	page := c.root.iter(byKey(k)).forEach(splitFullPage).Get()
	page.insert(k, value)

	if err := c.flushWriteBuffer(); err != nil {
		return err
	}

	return nil
}

// Update the value associated with key `k`. If no record
// with that key exists, an error is returned.
func (c *Collection) Update(k string, value []byte) error {
	// TODO: IMPLEMENT
	return nil
}

// Delete record with key `k`. An error is returned of no
// such record exists
func (c *Collection) Delete(k string) error {
	page := c.root.iter(byKey(k)).forEach(handleSparsePage).Get()

	if err := page.Delete(k); err != nil {
		return err
	}

	if err := c.flushWriteBuffer(); err != nil {
		return err
	}

	if c.root.Empty() && !c.root.Leaf() {
		c.root = c.root.children[0]
		c.RootPage = c.root.ID
		return c.save()
	}

	return nil
}

func (c *Collection) save() error {
	if c.s == nil {
		return fmt.Errorf("Collection has no reference to parent store")
	}

	return c.s.writeCollection(c)
}

func (c *Collection) newPage() *Page {
	p := newPage(c.T)
	p.c = c
	return p
}

func (c *Collection) loadPage(p *Page) error {
	if c == nil {
		return fmt.Errorf("Page %s has no reference to parent collection", p.ID)
	}

	if p.loaded {
		return nil
	}

	data, err := ioutil.ReadFile(path.Join(c.baseDir, p.ID))
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(p)
	if err != nil {
		return err
	}

	p.loaded = true
	
	if !p.leaf {
		for _, child := range p.children {
			child.c = p.c
		}
	}
	fmt.Println("Loaded page", p.ID)
	fmt.Println(p)
	return nil
}

// Accept interface instead
func (c *Collection) flushWriteBuffer() error {
	defer func() {
		for p := range c.writeBuf {
			delete(c.writeBuf, p)
		}
	}()

	for p := range c.writeBuf {
		fmt.Printf("Writing page %s\n", p.ID)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(p)
		if err != nil {
			return err
		}

		err = os.WriteFile(path.Join(c.baseDir, p.ID), buf.Bytes(), 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Collection) writePage(p *Page) {
	if c == nil {
		return
	}

	c.writeBuf[p] = true
}
