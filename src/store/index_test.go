package store

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/namvu9/keylime/src/types"
)

func TestGet(t *testing.T) {
	ctx := context.Background()
	var (
		root = makePage(2, makeRecords("g"),
			makePage(2, []types.Record{
				types.New("a", nil),
				types.New("b", nil),
				types.New("c", []byte{100, 100, 100}),
			}),
			makePage(2, []types.Record{
				types.New("h", nil),
				types.New("i", []byte{99, 99, 99}),
				types.New("j", nil),
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

		ki.Insert(context.Background(), types.New("q", nil))

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
			recordA = types.New("d", []byte{99})
			recordB = types.New("r", []byte{99})
			recordC = types.New("z", []byte{99})
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
		oldRoot.records = append(oldRoot.records, types.New("5", nil))
		oldRoot.children = []*Page{
			ki.newPage(true),
			deleteMe,
		}

		oldRoot.children[0].records = append(oldRoot.children[0].records, types.New("2", nil))
		oldRoot.children[1].records = append(oldRoot.children[1].records, types.New("8", nil))

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
		recordA = types.New("a", nil)
		recordB = types.New("b", nil)
		recordC = types.New("c", nil)

		recordD = types.New("d", nil)
		recordE = types.New("e", nil)
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

func TestInsertOrderIndex(t *testing.T) {
	ctx := context.Background()
	t.Run("Normal insert", func(t *testing.T) {
		oi := newOrderIndex(2, nil)
		r := types.NewRecord("k")

		oi.Insert(ctx, r)

		headNode, _ := oi.Node(oi.Head)

		if got := len(headNode.Records); got != 1 {
			t.Errorf("n records, want %d got %d", 1, got)
		}

		if first := headNode.Records[0]; first != r || first.Key != "k" {
			t.Errorf("Expected first record to have key k, got %s", first.Key)
		}
	})

	t.Run("Insertion into full node", func(t *testing.T) {
		oi := newOrderIndex(2, nil)
		oldHead, _ := oi.Node(oi.Head)
		oldHead.Records = []*types.Record{nil, nil}

		r := types.NewRecord("o")
		oi.Insert(ctx, r)

		if oi.Head == oi.Tail {
			t.Errorf("Expected new node to be allocated")
		}

		if oi.Tail != oldHead.ID {
			t.Errorf("Expected old head to be new tail")
		}

		newHead, _ := oi.Node(oi.Head)
		if newHead.Next != oi.Tail {
			t.Errorf("New head does not reference old head")
		}

		if oldHead.Prev != oi.Head {
			t.Errorf("Old head does not reference new head")
		}
	})
}

func TestGetOrderIndex(t *testing.T) {
	ctx := context.Background()
	t.Run("Desc: n < records in index", func(t *testing.T) {
		oi := newOrderIndex(2, nil)

		d := types.NewRecord("d")
		d.Deleted = true

		oi.Insert(ctx, types.NewRecord("a"))
		oi.Insert(ctx, types.NewRecord("b"))
		oi.Insert(ctx, types.NewRecord("c"))
		oi.Insert(ctx, d)
		oi.Insert(ctx, types.NewRecord("e"))

		res := oi.Get(4, false)

		if len(res) != 4 {
			t.Errorf("Want %d Got %d", 4, len(res))
		}

		if got := res[0].Key; got != "e" {
			t.Errorf("0: Want key e, got %s", got)
		}
		if got := res[1].Key; got != "c" {
			t.Errorf("1: Want key c, got %s", got)
		}
		if got := res[2].Key; got != "b" {
			t.Errorf("2: Want key b, got %s", got)
		}
		if got := res[3].Key; got != "a" {
			t.Errorf("3: Want key a, got %s", got)
		}
	})

	t.Run("Desc: n > records in index", func(t *testing.T) {
		oi := newOrderIndex(2, nil)

		d := types.NewRecord("d")
		d.Deleted = true

		oi.Insert(ctx, types.NewRecord("a"))
		oi.Insert(ctx, types.NewRecord("b"))
		oi.Insert(ctx, types.NewRecord("c"))
		oi.Insert(ctx, d)
		oi.Insert(ctx, types.NewRecord("e"))

		res := oi.Get(100, false)

		if len(res) != 4 {
			t.Errorf("Want %d Got %d", 4, len(res))
		}

		if got := res[0].Key; got != "e" {
			t.Errorf("0: Want key e, got %s", got)
		}
		if got := res[1].Key; got != "c" {
			t.Errorf("1: Want key c, got %s", got)
		}
		if got := res[2].Key; got != "b" {
			t.Errorf("2: Want key b, got %s", got)
		}
		if got := res[3].Key; got != "a" {
			t.Errorf("3: Want key a, got %s", got)
		}
	})

	t.Run("Asc: n < records in index", func(t *testing.T) {
		oi := newOrderIndex(2, nil)

		d := types.NewRecord("d")
		d.Deleted = true

		oi.Insert(ctx, types.NewRecord("a"))
		oi.Insert(ctx, types.NewRecord("b"))
		oi.Insert(ctx, types.NewRecord("c"))
		oi.Insert(ctx, d)
		oi.Insert(ctx, types.NewRecord("e"))

		res := oi.Get(4, true)

		if len(res) != 4 {
			t.Errorf("Want %d Got %d", 4, len(res))
		}

		if got := res[0].Key; got != "a" {
			t.Errorf("0: Want key a, got %s", got)
		}
		if got := res[1].Key; got != "b" {
			t.Errorf("1: Want key b, got %s", got)
		}
		if got := res[2].Key; got != "c" {
			t.Errorf("2: Want key c, got %s", got)
		}
		if got := res[3].Key; got != "e" {
			t.Errorf("3: Want key e, got %s", got)
		}
	})

	t.Run("Asc: n > records in index", func(t *testing.T) {
		oi := newOrderIndex(2, nil)

		d := types.NewRecord("d")
		d.Deleted = true

		oi.Insert(ctx, types.NewRecord("a"))
		oi.Insert(ctx, types.NewRecord("b"))
		oi.Insert(ctx, types.NewRecord("c"))
		oi.Insert(ctx, d)
		oi.Insert(ctx, types.NewRecord("e"))

		res := oi.Get(100, true)

		if len(res) != 4 {
			t.Errorf("Want %d Got %d", 4, len(res))
		}

		if got := res[0].Key; got != "a" {
			t.Errorf("0: Want key a, got %s", got)
		}
		if got := res[1].Key; got != "b" {
			t.Errorf("1: Want key b, got %s", got)
		}
		if got := res[2].Key; got != "c" {
			t.Errorf("2: Want key c, got %s", got)
		}
		if got := res[3].Key; got != "e" {
			t.Errorf("3: Want key e, got %s", got)
		}
	})
}

func equal(a, b interface{}) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
