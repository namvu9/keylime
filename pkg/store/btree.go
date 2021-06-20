package store

import "github.com/namvu9/keylime/pkg/record"

type BTree struct {
	T        int
	root     *BNode
	storage  NodeReadWriter
	basePath string
	cr       ChangeReporter
}

func (b *BTree) Unload() {
	b.root = nil
	//loadTree(b)
}

func partitionMedian(nums []record.Record) (record.Record, []record.Record, []record.Record) {
	if nRecords := len(nums); nRecords%2 == 0 || nRecords < 3 {
		panic("Cannot partition an even number of records")
	}
	medianIndex := (len(nums) - 1) / 2
	return nums[medianIndex], nums[:medianIndex], nums[medianIndex+1:]
}

func (bt *BTree) splitRoot() {
	s := bt.newNode()
	s.children = []*BNode{bt.root}
	bt.root = s
	s.splitChild(0)

	s.registerWrite("Split root")
}

func (bt *BTree) splitDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(func(parent, child *BNode, i int) {
		if child.isFull() {
			parent.splitChild(i)
			index, ok := parent.keyIndex(k)
			if ok {
				iter.nextChild = parent
			} else {
				iter.nextChild = parent.children[index]
			}
		}
	}).run()
	return node
}

// TODO: TEST
func handleSparseNode(node, child *BNode, index int) {
	var (
		p = node.childPredecessor(index)
		s = node.childSuccessor(index)
	)

	child.registerWrite("Sparse node")
	node.registerWrite("Sparse node (parent)")

	// Rotate predecessor key
	if p != nil && !p.isSparse() {
		p.registerWrite("Sparse node (predecessor)")
		var (
			recordIndex   = index - 1
			pivot         = node.records[recordIndex]
			siblingRecord = p.records[len(p.records)-1]
		)

		child.insertRecord(pivot)
		node.setRecord(recordIndex, siblingRecord)

		if !p.Leaf {
			// Move child from sibling to child
			siblingLastChild := p.children[len(p.children)-1]
			child.children = append([]*BNode{siblingLastChild}, child.children...)
			p.children = p.children[:len(p.children)-1]
		}
	} else if s != nil && !s.isSparse() {
		s.registerWrite("Sparse node (successor)")
		var (
			pivot         = node.records[index]
			siblingRecord = s.records[0]
		)

		// Move key from parent to child
		child.records = append(child.records, pivot)
		node.setRecord(index, siblingRecord)

		// Move child from sibling to child
		if !s.Leaf {
			siblingFirstChild := s.children[0]
			child.children = append(child.children, siblingFirstChild)
			s.children = s.children[1:]
		}
	} else if p != nil {
		node.mergeChildren(index - 1)
	} else {
		node.mergeChildren(index)
	}
	// Write nodes
}

// Descend the tree until either the key is found or a leaf
// node is found.
func (bt *BTree) mergeDescend(k string) *BNode {
	iter := bt.iter(k)
	node, _, _ := iter.forEach(func(parent, child *BNode, i int) {
		if child.isSparse() {
			handleSparseNode(parent, child, i)
			index, ok := parent.keyIndex(k)
			if ok {
				iter.nextChild = parent
			} else {
				iter.nextChild = parent.children[index]
			}
		}
	}).run()
	return node
}

func (bt *BTree) Set(k string, value []byte) error {
	defer func() {
		if r := recover(); r != nil {
			// Roll back
		}
	}()

	if bt.root.isFull() {
		bt.splitRoot()
	}

	node := bt.splitDescend(k)
	node.insertKey(k, value)

	// Commit writes
	return nil
}

func (t *BTree) Get(key string) []byte {
	if node, index := t.Search(key); node != nil {
		return node.records[index].Value()
	}

	return nil
}

type BTreeIterator struct {
	done      bool
	key       string
	node      *BNode
	fn        func(parent, child *BNode, childIndex int)
	nextChild *BNode
}

func (bti *BTreeIterator) forEach(fn func(*BNode, *BNode, int)) *BTreeIterator {
	bti.fn = fn
	return bti
}

func (bti *BTreeIterator) run() (*BNode, bool, int) {
	for {
		index, exists := bti.node.keyIndex(bti.key)
		if exists || bti.node.Leaf {
			bti.done = true
			return bti.node, exists, index
		}

		child := bti.node.children[index]
		if err := child.read(); err != nil {
			panic(err)
		}

		if bti.fn != nil {
			bti.fn(bti.node, child, index)

		}

		if bti.nextChild != nil {
			bti.node = bti.nextChild
			bti.nextChild = nil
		} else {
			bti.node = child
		}

	}
}

func (t *BTree) iter(key string) *BTreeIterator {
	return &BTreeIterator{
		key:  key,
		node: t.root,
	}
}

func (t *BTree) Search(key string) (*BNode, int) {
	node, ok, index := t.iter(key).run()

	if !ok {
		return nil, index
	}

	return node, index
}

func (b *BTree) Delete(k string) error {
	node := b.mergeDescend(k)
	err := node.deleteKey(k)
	if len(b.root.records) == 0 && len(b.root.children) == 1 {
		b.root = b.root.children[0]
	}

	if err != nil {
		// Roll back
		// Wrap err
		return err
	}

	return nil
}

type Option func(*BTree)

func WithStorage(s NodeReadWriter) Option {
	return func(b *BTree) {
		b.storage = s
	}
}

func WithBasePath(path string) Option {
	return func(b *BTree) {
		b.basePath = path
	}
}

func WithRoot(root *BNode) Option {
	return func(b *BTree) {
		b.root = root
	}
}

type ChangeReporter struct {
	writes  []*BNode
	deletes []*BNode
}

func (cr *ChangeReporter) Write(b *BNode, reason string) {
	for _, write := range cr.writes {
		if write.ID == b.ID {
			return
		}
	}
	cr.writes = append(cr.writes, b)
}

func (cr *ChangeReporter) Delete(b *BNode, reason string) {
	for _, del := range cr.writes {
		if del.ID == b.ID {
			return
		}
	}
	cr.deletes = append(cr.deletes, b)
}

func (b *BTree) newNode() *BNode {
	node := newNode(b.T)
	node.storage = &b.cr
	return node
}

// TODO: TEST
func New(t int, opts ...Option) *BTree {
	tree := &BTree{
		T:  t,
		cr: ChangeReporter{},
	}

	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = tree.newNode()
		tree.root.Leaf = true
	}

	return tree
}
