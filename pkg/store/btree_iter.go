package store

type BTreeIterator struct {
	key       string
	node      *BNode
}

func (bti *BTreeIterator) forEach(fn func(*BNode, *BNode, int) bool) (*BNode, bool, int) {
	for {
		index, exists := bti.node.keyIndex(bti.key)
		if exists || bti.node.leaf {
			return bti.node, exists, index
		}

		child := bti.node.children[index]

		if modified := fn(bti.node, child, index); modified {
			if index, ok := bti.node.keyIndex(bti.key); !ok {
				bti.node = bti.node.children[index]
			}
		} else {
			bti.node = child
		}
	}
}

func (bti *BTreeIterator) find() (*BNode, bool, int) {
	return bti.forEach(func(b1, b2 *BNode, i int) bool { return false })
}
