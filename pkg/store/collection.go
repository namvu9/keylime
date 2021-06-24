package store

// A Collection is a named container for a group of records
type Collection struct {
	t        int
	basePath string
	root     *Page
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(k string) []byte {
	node := c.root.iter(ByKey(k)).Get()
	index, ok := node.keyIndex(k)
	if !ok {
		return nil
	}

	return node.records[index].Value()
}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(k string, value []byte) error {
	if c.root.Full() {
		s := c.newPage()
		s.children = []*Page{c.root}
		c.root = s
		s.splitChild(0)
	}

	node := c.root.iter(ByKey(k)).forEach(splitFullPage).Get()
	node.insert(k, value)

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
	page := c.root.iter(ByKey(k)).forEach(handleSparsePage).Get()

	if err := page.Delete(k); err != nil {
		return err
	}

	if c.root.Empty() && !c.root.Leaf() {
		c.root = c.root.children[0]
	}

	return nil
}

func (c *Collection) newPage() *Page {
	node := newPage(c.t)
	return node
}
