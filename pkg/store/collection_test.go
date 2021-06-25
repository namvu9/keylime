package store

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/namvu9/keylime/pkg/record"
)

func TestGet(t *testing.T) {
	var (
		root = makePage(2, makeRecords("10"),
			makePage(2, makeRecords("1", "4", "8")),
			makePage(2, makeRecords("12", "16", "20")),
		)
		tree = &Collection{root: root}
	)

	root.children[0].records[1] = record.New(root.children[0].records[1].Key, []byte{99, 99, 99})
	root.children[1].records[2] = record.New(root.children[1].records[2].Key, []byte{100, 100, 100})

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

func TestSet(t *testing.T) {
	u := util{t}

	t.Run("Insertion into tree with full root and full target leaf", func(t *testing.T) {
		var (
			root = makePage(2, makeRecords("k", "o", "s"),
				makePage(2, makeRecords("a", "b", "c")),
				makePage(2, makeRecords("l", "m")),
				makePage(2, makeRecords("p", "q")),
				makePage(2, makeRecords("x", "y")),
			)
			tree = NewCollection(2, WithRoot(root))
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

func TestDelete(t *testing.T) {
	t.Run("Delete missing key", func(t *testing.T) {
		tree := NewCollection(2, WithRoot(makePage(2, makeRecords("5"))))

		err := tree.Delete("10")
		if err == nil {
			t.Errorf("Deleting a missing key should return an error. Got=<nil>")
		}
	})

	t.Run("Delete key from tree with a single key", func(t *testing.T) {
		u := util{t}
		tree := NewCollection(2, WithRoot(
			makePage(2, makeRecords("5")),
		))

		tree.Delete("5")
		u.hasNRecords("Root", 0, tree.root)
		u.hasNChildren("Root", 0, tree.root)
	})
	// Case x: Delete non-existing key
	// Case 0: Delete from root with 1 key
	t.Run("Delete from root with 1 key", func(t2 *testing.T) {
		u := util{t2}
		tree := NewCollection(2, WithRoot(makePage(2, makeRecords("5"),
			makePage(2, makeRecords("2")),
			makePage(2, makeRecords("8")),
		)))

		tree.Delete("5")

		u.with("Root", tree.root, func(nu namedUtil) {
			nu.hasKeys("2", "8")
			nu.hasNChildren(0)
		})
	})

	// Case 1: Delete From Leaf
	t.Run("Delete from leaf", func(t *testing.T) {
		var (
			root = makePage(2, makeRecords("1", "2", "3"))
			tree = &Collection{root: root}
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

func TestBuildCollection(t *testing.T) {
	u := util{t}

	collection := NewCollection(2)

	collection.Set("a", nil)
	collection.Set("b", nil)
	collection.Set("c", nil)

	u.with("Root after 3 insertions, t=2", collection.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a", "b", "c")
	})

	collection.Set("d", nil)
	collection.Set("e", nil)

	u.with("Root after 5 insertions", collection.root, func(nu namedUtil) {
		nu.hasNChildren(2)
		nu.hasKeys("b")
	})

	u.with("Left child after 5 insertions", collection.root.children[0], func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a")
	})

	u.with("Right child after 5 insertions", collection.root.children[1], func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("c", "d", "e")
	})

	collection.Delete("e")
	collection.Delete("d")
	collection.Delete("c")

	u.with("Root after deleting 3 times", collection.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a", "b")
	})

	collection.Delete("a")
	collection.Delete("b")

	u.with("Root should be empty", collection.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasNRecords(0)
	})
}
