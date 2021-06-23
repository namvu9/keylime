package store

type Collection struct {
	t        int
	basePath string
	root     *Page
}

func (c *Collection) Get(key string) []byte {
	node := c.root.Iter(ByKey(key)).Get()
	index, ok := node.keyIndex(key)
	if !ok {
		return nil
	}

	return node.records[index].Value()
}

func (c *Collection) Set(k string, value []byte) error {
	if c.root.Full() {
		s := c.newPage()
		s.children = []*Page{c.root}
		c.root = s
		s.splitChild(0)
	}

	node := c.root.Iter(ByKey(k)).ForEach(splitFullPage).Get()
	node.insert(k, value)

	return nil
}

func (c *Collection) Delete(k string) error {
	page := c.root.Iter(ByKey(k)).ForEach(handleSparsePage).Get()

	if err := page.Delete(k); err != nil {
		return err
	}

	if c.root.Empty() && !c.root.Leaf() {
		c.root = c.root.children[0]
	}

	return nil
}

func (b *Collection) newPage() *Page {
	node := newPage(b.t)
	return node
}

func New(t int, opts ...Option) *Collection {
	tree := &Collection{
		t: t,
	}

	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = tree.newPage()
		tree.root.leaf = true
	}

	return tree
}

