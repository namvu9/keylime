package store

import (
	"bytes"
	"context"
	"testing"

	record "github.com/namvu9/keylime/src/types"
)

func TestGet(t *testing.T) {
	ctx := context.Background()
	var (
		root = makePage(2, makeRecords("g"),
			makePage(2, []record.Record{
				record.New("a", nil),
				record.New("b", nil),
				record.New("c", []byte{100, 100, 100}),
			}),
			makePage(2, []record.Record{
				record.New("h", nil),
				record.New("i", []byte{99, 99, 99}),
				record.New("j", nil),
			}),
		)
		ki = newKeyIndex(2, nil)
	)

	ki.root = root

	for i, test := range []struct {
		k    string
		want []byte
	}{
		{"0", nil},
		{"5", nil},
		{"1000", nil},
		{"c", []byte{100, 100, 100}},
		{"i", []byte{99, 99, 99}},
	} {
		got, _ := ki.Get(ctx, test.k)

		if test.want == nil && got != nil {
			t.Errorf("Expected nil, got=%v", got)
		}

		if test.want != nil && got == nil {
			t.Errorf("%s: should not have been nil", test.k)
		}

		if test.want != nil && bytes.Compare(got.Value, test.want) != 0 {
			t.Errorf("[TestGet] %d, key %v: Got %s; want %s ", i, test.k, got.Value, test.want)
		}
	}
}

func TestInsert(t *testing.T) {
	u := util{t}

	t.Run("KI is saved if root changes", func(t *testing.T) {
		reporter := newIOReporter()
		ki := newKeyIndex(2, reporter)
		ki.root.records = makeRecords("k", "o", "s")

		ki.Insert(context.Background(), record.New("q", nil))

		u.with("Root", ki.root, func(nu namedUtil) {
			nu.hasNChildren(2)
		})

		if !reporter.writes["key_index"] {
			t.Errorf("KeyIndex not written")
		}

		if !reporter.writes[ki.root.ID] {
			t.Errorf("New root not written")
		}

		if !reporter.writes[ki.root.children[0].ID] {
			t.Errorf("New root not written")
		}

		if !reporter.writes[ki.root.children[1].ID] {
			t.Errorf("New root not written")
		}

		if ki.RootPage != ki.root.ID {
			t.Errorf("Want=%s Got=%s", ki.root.ID, ki.RootPage)
		}

	})

	t.Run("Insertion into tree with full root and full target leaf", func(t *testing.T) {
		var (
			root = makePage(2, makeRecords("k", "o", "s"),
				makePage(2, makeRecords("a", "b", "c")),
				makePage(2, makeRecords("l", "m")),
				makePage(2, makeRecords("p", "q")),
				makePage(2, makeRecords("x", "y")),
			)
			ki = newKeyIndex(2, nil)
		)

		ki.root = root

		var (
			recordA = record.New("d", []byte{99})
			recordB = record.New("r", []byte{99})
			recordC = record.New("z", []byte{99})
		)

		ctx := context.Background()

		ki.Insert(ctx, recordA)
		ki.Insert(ctx, recordB)
		ki.Insert(ctx, recordC)

		if ki.root == root {
			t.Errorf("[TestTreeInsert]: Expected new root")
		}

		u.with("root", ki.root, func(nu namedUtil) {
			nu.hasNChildren(2)
			nu.hasKeys("o")
		})

		rootSibling := ki.root.children[1]
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
	ctx := context.Background()
	u := util{t}

	t.Run("KI is saved if root becomes empty", func(t *testing.T) {
		reporter := newIOReporter()
		ki := newKeyIndex(2, reporter)
		deleteMe := ki.newPage(true)

		oldRoot := ki.newPage(false)
		oldRoot.records = append(oldRoot.records, record.New("5", nil))
		oldRoot.children = []*Page{
			ki.newPage(true),
			deleteMe,
		}

		oldRoot.children[0].records = append(oldRoot.children[0].records, record.New("2", nil))
		oldRoot.children[1].records = append(oldRoot.children[1].records, record.New("8", nil))

		ki.root = oldRoot

		ki.Delete(ctx, "5")

		u.with("Root", ki.root, func(nu namedUtil) {
			nu.hasNChildren(0)
		})

		if len(reporter.writes) != 2 {
			t.Errorf("Want=2 Got=%d", len(reporter.writes))
		}

		if !reporter.writes["key_index"] {
			t.Errorf("KeyIndex not written")
		}

		if !reporter.writes[ki.root.ID] {
			t.Errorf("New root not written")
		}

		if len(reporter.deletes) != 2 {
			t.Errorf("Want=1 Got=%d", len(reporter.deletes))
		}
		if !reporter.deletes[oldRoot.ID] {
			t.Errorf("Old root not deleted")
		}
		if !reporter.deletes[deleteMe.ID] {
			t.Errorf("Old root not deleted")
		}
	})

	t.Run("Delete missing key", func(t *testing.T) {
		ki := newKeyIndex(2, nil)
		ki.root = makePage(2, makeRecords("5"))

		err := ki.Delete(ctx, "10")
		if err == nil {
			t.Errorf("Deleting a missing key should return an error. Got=<nil>")
		}
	})

	t.Run("Delete key from tree with a single key", func(t *testing.T) {
		u := util{t}
		ki := newKeyIndex(2, nil)
		ki.root = makePage(2, makeRecords("5"))

		ki.Delete(ctx, "5")
		u.hasNRecords("Root", 0, ki.root)
		u.hasNChildren("Root", 0, ki.root)
	})

	// Case x: Delete non-existing key
	// Case 0: Delete from root with 1 key
	t.Run("Delete from root with 1 key", func(t2 *testing.T) {
		u := util{t2}
		ki := newKeyIndex(2, nil)
		ki.root = makePage(2, makeRecords("5"),
			makePage(2, makeRecords("2")),
			makePage(2, makeRecords("8")),
		)

		ki.Delete(ctx, "5")

		u.with("Root", ki.root, func(nu namedUtil) {
			nu.hasKeys("2", "8")
			nu.hasNChildren(0)
		})
	})

	// Case 1: Delete From Leaf
	t.Run("Delete from leaf", func(t *testing.T) {
		ki := newKeyIndex(2, nil)
		root := makePage(2, makeRecords("1", "2", "3"))
		ki.root = root

		ki.Delete(ctx, "2")

		u := util{t}
		u.with("leaf", root, func(nu namedUtil) {
			nu.hasNRecords(2)
			nu.hasKeys("1", "3")
			nu.hasNChildren(0)
		})
	})
}

func TestBuildKeyIndex(t *testing.T) {
	u := util{t}

	ki := newKeyIndex(2, nil)
	ctx := context.Background()

	var (
		recordA = record.New("a", nil)
		recordB = record.New("b", nil)
		recordC = record.New("c", nil)

		recordD = record.New("d", nil)
		recordE = record.New("e", nil)
	)

	ki.Insert(ctx, recordA)
	ki.Insert(ctx, recordB)
	ki.Insert(ctx, recordC)

	u.with("Root after 3 insertions, t=2", ki.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a", "b", "c")
	})

	ki.Insert(ctx, recordD)
	ki.Insert(ctx, recordE)

	u.with("Root after 5 insertions", ki.root, func(nu namedUtil) {
		nu.hasNChildren(2)
		nu.hasKeys("b")
	})

	u.with("Left child after 5 insertions", ki.root.children[0], func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a")
	})

	u.with("Right child after 5 insertions", ki.root.children[1], func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("c", "d", "e")
	})

	ki.Delete(ctx, "e")
	ki.Delete(ctx, "d")
	ki.Delete(ctx, "c")

	u.with("Root after deleting 3 times", ki.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasKeys("a", "b")
	})

	ki.Delete(ctx, "a")
	ki.Delete(ctx, "b")

	u.with("Root should be empty", ki.root, func(nu namedUtil) {
		nu.hasNChildren(0)
		nu.hasNRecords(0)
	})
}
