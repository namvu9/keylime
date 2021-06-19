package store

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSet(t *testing.T) {
	u := util{t}

	t.Run("Insertion into tree with full root and full target leaf", func(t *testing.T) {
		var (
			root = makeTree(2, makeRecords("k", "o", "s"),
				makeTree(2, makeRecords("a", "b", "c")),
				makeTree(2, makeRecords("l", "m")),
				makeTree(2, makeRecords("p", "q")),
				makeTree(2, makeRecords("x", "y")),
			)
			tree = &BTree{root, nil}
		)

		tree.Set("d", []byte{99})
		tree.Set("r", []byte{99})
		tree.Set("z", []byte{99})

		if tree.root == root {
			t.Errorf("[TestTreeInsert]: Expected new root")
		}

		u.with("root", tree.root, func(nu namedUtil) {
			nu.hasNChildren(2)
			nu.hasKeys("o")
		})

		rootSibling := tree.root.children[1]
		u.with("Root sibling", rootSibling, func(nu namedUtil) {
			nu.hasKeys("s")
			nu.hasNChildren(2)
		})

		u.hasKeys("Root sibling, child 0", []string{"p", "q", "r"}, rootSibling.children[0])
		u.hasKeys("Root sibling, child 0", []string{"x", "y", "z"}, rootSibling.children[1])

		u.with("Old root", root, func(nu namedUtil) {
			nu.hasKeys("b", "k")
			nu.hasNChildren(3)
		})

		u.hasKeys("Old root, child 0", []string{"a"}, root.children[0])
		u.hasKeys("Old root, child 1", []string{"c", "d"}, root.children[1])
		u.hasKeys("Old root, child 2", []string{"l", "m"}, root.children[2])
	})
}

func TestSearch(t *testing.T) {
	var (
		root = makeTree(2, makeRecords("10"),
			makeTree(2, makeRecords("1", "4", "8")),
			makeTree(2, makeRecords("12", "16", "20")),
		)
		tree = &BTree{root: root}
	)

	root.children[0].records[1].value = []byte{99, 99, 99}
	root.children[1].records[2].value = []byte{100, 100, 100}

	tree.Set("4", []byte{99, 99, 99})
	tree.Set("10", []byte("I'm not cool"))
	tree.Set("100", []byte(fmt.Sprint(4)))

	for i, test := range []struct {
		k    string
		want []byte
	}{
		{"0", nil},
		{"5", nil},
		{"1000", nil},
		{"4", []byte{99, 99, 99}},
		{"20", []byte{100, 100, 100}},
		{"10", []byte("I'm not cool")},
		{"100", []byte(fmt.Sprint(4))},
	} {

		got := tree.Get(test.k)

		if bytes.Compare(got, test.want) != 0 {
			t.Errorf("[TestSearch] %d, key %v: Got %s; want %s ", i, test.k, got, test.want)
		}
	}
}

func TestMergeDescend(t *testing.T) {

	t.Run("Left sibling has t keys", func(t *testing.T) {
		u := util{t}
		root := makeTree(2, makeRecords("c"),
			makeTree(2, makeRecords("a", "b")),
			makeTree(2, makeRecords("d")),
		)

		tree := &BTree{root, nil}
		tree.mergeDescend("d")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasKeys("b")
			nu.hasNChildren(2)
		})

		u.with("Right child", tree.root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d")
		})
	})

	t.Run("Left internal node sibling has t keys", func(t *testing.T) {
		u := util{t}
		movedChild := makeTree(2, makeRecords())
		root := makeTree(2, makeRecords("c"),
			makeTree(2, makeRecords("a", "b"),
				makeTree(2, makeRecords()),
				makeTree(2, makeRecords()),
				movedChild,
			),
			makeTree(2, makeRecords("d"),
				makeTree(2, makeRecords()),
				makeTree(2, makeRecords()),
			),
		)

		tree := &BTree{root, nil}
		tree.mergeDescend("d")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasKeys("b")
			nu.hasNChildren(2)
		})

		u.with("Right child", tree.root.children[1], func(nu namedUtil) {
			nu.hasNChildren(3)
			nu.hasKeys("c", "d")
			if nu.node.children[0] != movedChild {
				t.Errorf("Right child expected movedChild as its first child")
			}
		})
	})

	t.Run("Right sibling has t keys", func(t *testing.T) {
		u := util{t}
		root := makeTree(2, makeRecords("c"),
			makeTree(2, makeRecords("a")),
			makeTree(2, makeRecords("d", "e")),
		)

		tree := &BTree{root, nil}
		tree.mergeDescend("a")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)
		})

		u.with("Left child", tree.root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "c")
		})
	})

	t.Run("Right internal node sibling has t keys", func(t *testing.T) {
		u := util{t}
		movedChild := makeTree(2, makeRecords())
		root := makeTree(2, makeRecords("c"),
			makeTree(2, makeRecords("a"),
				makeTree(2, makeRecords()),
				makeTree(2, makeRecords()),
			),
			makeTree(2, makeRecords("d", "e"),
				movedChild,
				makeTree(2, makeRecords()),
				makeTree(2, makeRecords()),
			),
		)

		tree := &BTree{root, nil}
		tree.mergeDescend("a")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)
		})

		u.with("Left child", tree.root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "c")
			nu.hasNChildren(3)
			if nu.node.children[2] != movedChild {
				t.Errorf("LeftChild, expected movedChild as last child")
			}
		})
	})

	t.Run("Both siblings are sparse", func(t *testing.T) {
		u := util{t}
		root := makeTree(2, makeRecords("b", "d"),
			makeTree(2, makeRecords("a")),
			makeTree(2, makeRecords("c")),
			makeTree(2, makeRecords("e")),
		)

		tree := &BTree{root, nil}
		node := tree.mergeDescend("c")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", tree.root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
			nu.is(node)
		})
	})

	t.Run("Both siblings are sparse; no right sibling", func(t *testing.T) {
		u := util{t}
		root := makeTree(2, makeRecords("b", "d"),
			makeTree(2, makeRecords("a")),
			makeTree(2, makeRecords("c")),
			makeTree(2, makeRecords("e")),
		)

		tree := &BTree{root, nil}
		node := tree.mergeDescend("e")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", tree.root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d", "e")
			nu.is(node)
		})
	})

	t.Run("Both siblings are sparse; no left sibling", func(t *testing.T) {
		u := util{t}
		root := makeTree(2, makeRecords("b", "d"),
			makeTree(2, makeRecords("a")),
			makeTree(2, makeRecords("c")),
			makeTree(2, makeRecords("e")),
		)

		tree := &BTree{root, nil}
		node := tree.mergeDescend("a")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", tree.root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
			nu.is(node)
		})
	})
}

func TestDelete(t *testing.T) {
	t.Run("Delete key from tree with a single key", func(t *testing.T) {
		u := util{t}
		tree := &BTree{
			makeTree(2, makeRecords("5")), nil}

		tree.Delete("5")
		u.hasNRecords("Root", 0, tree.root)
		u.hasNChildren("Root", 0, tree.root)
	})
	// Case x: Delete non-existing key
	// Case 0: Delete from root with 1 key
	t.Run("Delete from root with 1 key", func(t2 *testing.T) {
		u := util{t2}
		tree := &BTree{
			makeTree(2, makeRecords("5"),
				makeTree(2, makeRecords("2")),
				makeTree(2, makeRecords("8")),
			), nil}

		tree.Delete("5")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasKeys("2", "8")
			nu.hasNChildren(0)
		})
	})

	// Case 1: Delete From Leaf
	t.Run("Delete from leaf", func(t *testing.T) {
		var (
			root = makeTree(2, makeRecords("1", "2", "3"))
			tree = &BTree{root: root}
		)

		tree.Delete("2")

		u := util{t}
		u.with("leaf", root, func(nu namedUtil) {
			nu.hasNRecords(2)
			nu.hasKeys("1", "3")
			nu.hasNChildren(0)
		})
	})
}
