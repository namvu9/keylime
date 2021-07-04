package store

import (
	"fmt"
	"testing"

	"github.com/namvu9/keylime/src/types"
)

func TestDeletePage(t *testing.T) {
	bs := newWriteBuffer(nil)
	makeBufPage := makePageWithBufferedStorage(bs)

	t.Run("Missing key", func(t *testing.T) {
		bs.flush()

		page := newPageWithKeys(2, []string{"a", "c"})
		page.writer = bs
		err := page.remove("b")
		if err == nil {
			t.Errorf("deleteKey should return error if key is not found")
		}

		if len(bs.writeBuf) != 0 {
			t.Errorf("Failed deletion should not schedule any writes")
		}
	})

	t.Run("Delete key in leaf", func(t *testing.T) {
		u := util{t}

		for index, test := range []struct {
			targetKey string
			want      []string
		}{
			{"a", []string{"b", "c"}},
			{"b", []string{"a", "c"}},
			{"c", []string{"a", "b"}},
		} {
			bs.flush()
			var (
				node = makeBufPage(2, makeDocs("a", "b", "c"))
			)

			err := node.remove(test.targetKey)
			if err != nil {
				t.Errorf("Should not return error")
			}

			u.hasKeys(fmt.Sprintf("[DeleteKey]: %d", index), test.want, node)

			if len(bs.writeBuf) != 1 {
				t.Errorf("Deleting from leaf: Want=%d, Got=%d", 1, len(bs.writeBuf))
			}

			if got := bs.writeBuf[node.ID]; got != node {
				t.Errorf("Schedule delete: Want=%p Got=%p", node, got)
			}
		}
	})

	t.Run("Internal node, predecessor has t keys", func(t *testing.T) {
		u := &util{t}
		bs.flush()

		root := makeBufPage(2, makeDocs("5"),
			makeBufPage(2, makeDocs("2", "3")),
			makeBufPage(2, makeDocs("6")),
		)
		root.writer = bs

		root.remove("5")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("3")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("2")
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("6")
		})

		if got := len(bs.writeBuf); got != 2 {
			t.Errorf("Buffered Writes: Got=%d, Want=%d", got, 2)
		}

		if got := bs.writeBuf[root.ID]; got == nil {
			t.Errorf("root.children[0] not written")
		}

		if got := bs.writeBuf[root.children[0].ID]; got == nil {
			t.Errorf("root.children[0] not written")
		}
	})

	t.Run("Deep internal node, predecessor has t keys", func(t *testing.T) {
		u := &util{t}
		bs.flush()

		predPage := makeBufPage(2, makeDocs("6"))
		mergePage := makeBufPage(2, makeDocs("4"))
		root := makeBufPage(2, makeDocs("9"),
			makeBufPage(2, makeDocs("3", "5"),
				makeBufPage(2, makeDocs("2")),
				mergePage,
				predPage,
			),
			makeBufPage(2, makeDocs("10000")),
		)

		root.remove("9")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("6")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasNChildren(2)
			nu.hasKeys("3")

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("2")
			})
			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("4", "5")
			})
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("10000")
		})

		if got := len(bs.writeBuf); got != 3 {
			t.Errorf("Buffered writes: Got=%d, Want=%d", got, 3)
		}

		if got := bs.writeBuf[root.ID]; got == nil {
			t.Errorf("Root not written")
		}

		if got := bs.writeBuf[root.children[0].ID]; got == nil {
			t.Errorf("Left child not written")
		}

		if got := bs.writeBuf[mergePage.ID]; got == nil {
			t.Errorf("MergePage not written")
		}

		if got := bs.deleteBuf[predPage.ID]; got == nil {
			t.Errorf("PredPage not deleted")
		}
	})

	t.Run("Internal node, successor has t keys", func(t *testing.T) {
		u := &util{t}
		bs.flush()

		root := makeBufPage(2, makeDocs("5"),
			makeBufPage(2, makeDocs("2")),
			makeBufPage(2, makeDocs("6", "7")),
		)

		root.remove("5")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("6")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("2")
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("7")
		})

		if got := len(bs.writeBuf); got != 2 {
			t.Errorf("Buffered Writes: Got=%d, Want=%d", got, 2)
		}

		if got := bs.writeBuf[root.ID]; got == nil {
			t.Errorf("Root not written")
		}

		if got := bs.writeBuf[root.children[1].ID]; got == nil {
			t.Errorf("root.children[1] not written")
		}
	})

	t.Run("Deep internal node, successor has t keys", func(t *testing.T) {
		u := &util{t}
		bs.flush()

		mergedNode := makeBufPage(2, makeDocs("4"))
		deleteNode := makeBufPage(2, makeDocs("7"))

		root := makeBufPage(2, makeDocs("3"),
			makeBufPage(2, makeDocs("10000")),
			makeBufPage(2, makeDocs("5", "8"),
				mergedNode,
				deleteNode,
				makeBufPage(2, makeDocs("9")),
			),
		)

		root.remove("3")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("4")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("10000")

		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(2)
			nu.hasKeys("8")

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("5", "7")
			})
			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("9")
			})
		})

		if got := len(bs.writeBuf); got != 3 {
			t.Errorf("Buffered writes: Got=%d, Want=%d", got, 3)
		}

		if got := bs.writeBuf[root.ID]; got == nil {
			t.Errorf("Root not written")
		}

		if got := bs.writeBuf[root.children[1].ID]; got == nil {
			t.Errorf("Left child not written")
		}

		if got := bs.writeBuf[mergedNode.ID]; got == nil {
			t.Errorf("MergePage not written")
		}

		if got := bs.deleteBuf[deleteNode.ID]; got == nil {
			t.Errorf("PredPage not deleted")
		}

	})

	t.Run("Internal node, predecessor and successor have t-1 keys", func(t *testing.T) {
		u := &util{t}
		bs.flush()

		deletePage := makeBufPage(2, makeDocs("6"))
		root := makeBufPage(2, makeDocs("5"),
			makeBufPage(2, makeDocs("2")),
			deletePage,
		)

		root.remove("5")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(0)
			nu.hasNChildren(1)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("2", "6")
				nu.hasNChildren(0)
			})
		})

		if len(bs.writeBuf) != 2 {
			t.Errorf("Want=%d Got=%d", 2, len(bs.writeBuf))
		}
		if got := bs.writeBuf[root.ID]; got == nil {
			t.Errorf("Root not written")
		}

		if got := bs.writeBuf[root.children[0].ID]; got == nil {
			t.Errorf("Left child not written")
		}

		if len(bs.deleteBuf) != 1 || bs.deleteBuf[deletePage.ID] == nil {
			t.Errorf("Want=%d Got=%d", 2, len(bs.deleteBuf))
		}

	})
}

func TestInsertRecord(t *testing.T) {
	u := util{t}
	bs := newWriteBuffer(nil)
	makeBufPage := makePageWithBufferedStorage(bs)

	for i, test := range []struct {
		k        string
		keys     []string
		wantKeys []string
	}{
		{"2", []string{"1", "3", "5"}, []string{"1", "2", "3", "5"}},
		{"0", []string{"1", "3", "5"}, []string{"0", "1", "3", "5"}},
		{"4", []string{"1", "3", "5"}, []string{"1", "3", "4", "5"}},
		{"6", []string{"1", "3", "5"}, []string{"1", "3", "5", "6"}},
		{"10", []string{"1", "3", "5"}, []string{"1", "10", "3", "5"}},
	} {
		bs.flush()

		root := makeBufPage(3, makeDocs(test.keys...))

		root.insert(types.NewDoc(test.k))

		u.hasKeys(fmt.Sprintf("TestLeafInsert %d", i), test.wantKeys, root)

		if len(bs.writeBuf) != 1 || bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}
	}
}

func TestSplitChild(t *testing.T) {
	u := util{t}
	bs := newWriteBuffer(nil)
	makeBufPage := makePageWithBufferedStorage(bs)

	t.Run("Full leaf child", func(t *testing.T) {
		fullChild := makeBufPage(2, makeDocs("12", "14", "20"))
		var (
			root = makeBufPage(2, makeDocs("10"),
				makeBufPage(2, makeDocs("1", "4", "8")),
				fullChild,
			)
		)

		root.splitChild(1)

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("10", "14")
			nu.hasNChildren(3)
		})
		newChild := root.children[2]

		u.with("Full child", fullChild, func(nu namedUtil) {
			nu.hasKeys("12")
		})

		u.with("New child", newChild, func(nu namedUtil) {
			nu.hasKeys("20")
		})

		if len(bs.writeBuf) != 3 {
			t.Errorf("Want=%d Got=%d", 3, len(bs.writeBuf))
		}

		if bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}

		if bs.writeBuf[fullChild.ID] == nil {
			t.Errorf("Full child not written")
		}

		if bs.writeBuf[newChild.ID] == nil {
			t.Errorf("New child not written")
		}

		if len(bs.deleteBuf) != 0 {
			t.Errorf("Want=%d Got=%d", 0, len(bs.deleteBuf))
		}
	})

	t.Run("Full internal node", func(t *testing.T) {
		bs.flush()

		l2a_child := makeBufPage(2, makeDocs("6", "7"))
		l2b_child := makeBufPage(2, makeDocs("9", "10"))
		l2c_child := makeBufPage(2, makeDocs("16", "17"))
		l2d_child := makeBufPage(2, makeDocs("19", "20"))
		root := makeBufPage(2, makeDocs("21"),
			makeBufPage(2, makeDocs("8", "15", "18"),
				l2a_child,
				l2b_child,
				l2c_child,
				l2d_child,
			),
			makeBufPage(2, makeDocs()),
		)

		root.splitChild(0)

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("15", "21")
			nu.hasNChildren(3)
		})

		u.with("L1 children", root.children[0], func(nu namedUtil) {
			nu.hasChildren(l2a_child, l2b_child)
			nu.hasKeys("8")
		})

		newChild := root.children[1]
		u.with("New child", newChild, func(nu namedUtil) {
			nu.hasKeys("18")
			nu.hasChildren(l2c_child, l2d_child)
		})

		if got := len(bs.writeBuf); got != 3 {
			t.Errorf("Got=%d Want=3", got)
		}

		if got := len(bs.deleteBuf); got != 0 {
			t.Errorf("Got=%d Want=3", got)
		}

		if bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}

		if bs.writeBuf[root.children[0].ID] == nil {
			t.Errorf("Full child not written")
		}

		if bs.writeBuf[newChild.ID] == nil {
			t.Errorf("New child not written")
		}
	})

	t.Run("Full leaf child 2", func(t *testing.T) {
		bs.flush()

		root := makeBufPage(2, makeDocs("1", "3"),
			makeBufPage(2, makeDocs("0")),
			makeBufPage(2, makeDocs("2")),
			makeBufPage(2, makeDocs("4", "5", "6")),
		)

		root.splitChild(2)

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("1", "3", "5")
			nu.hasNChildren(4)
		})

		u.with("Left-most child", root.children[0], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("0")
		})

		u.with("child[1]", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("2")
		})

		u.with("child[2]", root.children[2], func(nu namedUtil) {
			nu.hasNChildren(0)
			nu.hasKeys("4")
		})

		newChild := root.children[3]
		u.with("New child", newChild, func(nu namedUtil) {
			nu.hasKeys("6")
			nu.hasNChildren(0)
		})
	})
}

func TestInsertChild(t *testing.T) {
	u := util{t}
	t.Run("Prepend", func(t *testing.T) {
		var (
			childA   = newPageWithKeys(2, []string{"2"})
			childC   = newPageWithKeys(2, []string{"8"})
			newChild = newPageWithKeys(2, []string{"10"})
			root     = makePage(2, makeDocs("5"),
				childA,
				childC,
			)
		)

		root.insertChildren(0, newChild)

		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(newChild, childA, childC)
		})
	})

	t.Run("Insert (middle)", func(t *testing.T) {
		var (
			root     = newPageWithKeys(2, []string{"5"})
			childA   = newPageWithKeys(2, []string{"2"})
			childC   = newPageWithKeys(2, []string{"8"})
			newChild = newPageWithKeys(2, []string{"10"})
		)

		root.children = []*Page{childA, childC}
		root.insertChildren(1, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, newChild, childC)
		})
	})

	t.Run("Append", func(t *testing.T) {
		var (
			root     = newPageWithKeys(2, []string{"5"})
			childA   = newPageWithKeys(2, []string{"2"})
			childC   = newPageWithKeys(2, []string{"8"})
			newChild = newPageWithKeys(2, []string{"10"})
		)

		root.children = []*Page{childA, childC}
		root.insertChildren(2, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, childC, newChild)
		})
	})

	t.Run("Empty", func(t *testing.T) {
		var (
			root     = newPageWithKeys(2, []string{"5"})
			newChild = newPageWithKeys(2, []string{"10"})
		)

		root.insertChildren(0, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(newChild)
		})
	})

	t.Run("Multiple into empty", func(t *testing.T) {
		u := util{t}
		var (
			root   = newPageWithKeys(2, []string{"5"})
			childA = newPageWithKeys(2, []string{"2"})
			childB = newPageWithKeys(2, []string{"2"})
			childC = newPageWithKeys(2, []string{"8"})
		)

		root.insertChildren(0, childA, childB, childC)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, childB, childC)
		})
	})

	t.Run("Insert multiple", func(t *testing.T) {
		u := util{t}
		var (
			childA = newPageWithKeys(2, []string{"2"})
			childB = newPageWithKeys(2, []string{"2"})
			childC = newPageWithKeys(2, []string{"8"})

			newChildA = newPageWithKeys(2, []string{"8"})
			newChildB = newPageWithKeys(2, []string{"8"})

			root = makePage(2, makeDocs("5"),
				childA,
				childB,
				childC,
			)
		)

		root.insertChildren(1, newChildA, newChildB)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, newChildA, newChildB, childB, childC)
		})
	})
}

func TestPageIndex(t *testing.T) {
	for i, test := range []struct {
		k          string
		keys       []string
		wantIndex  int
		wantExists bool
	}{
		{"0", []string{"1", "2", "3"}, 0, false},
		{"1", []string{"1", "2", "3"}, 0, true},
		{"4", []string{"1", "2", "3"}, 3, false},
		{"4", []string{"1", "2", "4"}, 2, true},
		{"3", []string{"1", "2", "4"}, 2, false},
		{"10", []string{"10", "5"}, 0, true},
	} {
		root := newPage(100, true, nil)
		root.docs = makeDocs(test.keys...)
		gotIndex, gotExists := root.keyIndex(test.k)

		if gotIndex != test.wantIndex || gotExists != test.wantExists {
			t.Errorf("[TestKeyIndex] %d: Got (%v, %v); Want (%v, %v)", i, gotIndex, gotExists, test.wantIndex, test.wantExists)
		}
	}
}

func TestMergeChildren(t *testing.T) {
	u := util{t}
	root := makePage(2, makeDocs("5", "10", "15"),
		makePage(2, makeDocs("2"),
			makePage(2, makeDocs()),
			makePage(2, makeDocs()),
		),
		makePage(2, makeDocs("7", "8"),
			makePage(2, makeDocs()),
			makePage(2, makeDocs()),
			makePage(2, makeDocs()),
		),
		makePage(2, makeDocs("11"),
			makePage(2, makeDocs()),
			makePage(2, makeDocs()),
		),
		makePage(2, makeDocs("16"),
			makePage(2, makeDocs()),
			makePage(2, makeDocs()),
		),
	)

	root.mergeChildren(1)

	u.with("Root", root, func(nu namedUtil) {
		nu.hasKeys("5", "15")
		nu.hasNChildren(3)
	})

	u.with("Left child", root.children[0], func(nu namedUtil) {
		nu.hasKeys("2")
		nu.hasNChildren(2)
	})

	u.with("mergedChild", root.children[1], func(nu namedUtil) {
		nu.hasKeys("7", "8", "10", "11")
		nu.hasNChildren(5)
	})

	u.with("Right child", root.children[2], func(nu namedUtil) {
		nu.hasKeys("16")
		nu.hasNChildren(2)
	})
}

func TestPredecessorSuccessorPage(t *testing.T) {
	target := makePage(2, makeDocs("99"))
	root := makePage(2, makeDocs("a", "c"),
		makePage(2, makeDocs()),
		target,
		makePage(2, makeDocs()),
	)

	if pred, _ := root.predecessorPage("c"); pred != target {
		t.Errorf("%v", root)
	}

	pred, _ := root.predecessorPage("c")
	succ, _ := root.successorPage("a")
	if pred != succ {
		t.Errorf("root.predecessorKeyNode(index) should be root.successorKeyNode(index-1)")
	}
}

func TestFull(t *testing.T) {
	root := newPage(2, true, nil)

	if got := root.full(); got {
		t.Errorf("New(2).IsFull() = %v; want false", got)
	}

	root.docs = makeDocs("1", "2", "3")

	if got := root.full(); !got {
		t.Errorf("Want root.IsFull() = true, got %v", got)
	}
}

func TestSparse(t *testing.T) {
	for i, test := range []struct {
		t          int
		keys       []string
		wantSparse bool
	}{
		{3, []string{"1"}, true},
		{3, []string{"1", "2"}, true},
		{3, []string{"1", "2", "3"}, false},
	} {
		node := newPageWithKeys(test.t, test.keys)

		if got := node.sparse(); got != test.wantSparse {
			t.Errorf("%d: Got=%v; Want=%v", i, got, test.wantSparse)
		}

	}
}

func TestChildSibling(t *testing.T) {
	var (
		child   = makePage(2, makeDocs())
		sibling = makePage(2, makeDocs())
		root    = makePage(2, makeDocs("c", "e", "f"),
			makePage(2, makeDocs()),
			child,
			sibling,
			makePage(2, makeDocs()),
		)
	)

	if root.prevChildSibling(0) != nil {
		t.Errorf("Left-most child has no left sibling")
	}

	if root.nextChildSibling(3) != nil {
		t.Errorf("Right-most child has no right sibling")
	}

	if root.nextChildSibling(1) != sibling {
		t.Errorf("We riot")
	}

	if root.prevChildSibling(2) != child {
		t.Errorf("We riot")
	}

	if root.nextChildSibling(1) != root.prevChildSibling(3) {
		t.Errorf("We riot")
	}
}

func TestSplitFullPage(t *testing.T) {
	u := util{t}

	t.Run("1", func(t *testing.T) {
		root := makePage(2, makeDocs("3"), makePage(2, makeDocs("a")), makePage(2, makeDocs("5", "7", "9")))

		splitFullPage(root, root.children[1])

		u.with("1", root, func(nu namedUtil) {
			nu.hasKeys("3", "7")
			nu.hasNChildren(3)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a")
			})
			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("5")
			})
			nu.withChild(2, func(nu namedUtil) {
				nu.hasKeys("9")
			})
		})
	})

	t.Run("2", func(t *testing.T) {
		root := makePage(2, makeDocs("9"), makePage(2, makeDocs("3", "5", "8")), makePage(2, makeDocs("a")))

		splitFullPage(root, root.children[0])

		u.with("2", root, func(nu namedUtil) {
			nu.hasKeys("5", "9")
			nu.hasNChildren(3)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("3")
			})
			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("8")
			})
			nu.withChild(2, func(nu namedUtil) {
				nu.hasKeys("a")
			})
		})
	})

	t.Run("3", func(t *testing.T) {
		root := makePage(2, makeDocs("9"), makePage(2, makeDocs("3", "8")), makePage(2, makeDocs("a")))

		splitFullPage(root, root.children[0])

		u.with("3", root, func(nu namedUtil) {
			nu.hasKeys("9")
			nu.hasNChildren(2)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("3", "8")
			})
			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("a")
			})
		})
	})
}

func TestHandleSparsePage(t *testing.T) {
	bs := newWriteBuffer(nil)
	makeBufPage := makePageWithBufferedStorage(bs)

	t.Run("Left sibling has t keys", func(t *testing.T) {
		u := util{t}
		bs.flush()

		root := makeBufPage(2, makeDocs("c"),
			makeBufPage(2, makeDocs("a", "b")),
			makeBufPage(2, makeDocs("d")),
		)

		handleSparsePage(root, root.children[1])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("b")
			nu.hasNChildren(2)
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d")
		})

		if got := len(bs.writeBuf); got != 3 {
			t.Errorf("Got=%d, Want=3", got)
		}

		if bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}

		if bs.writeBuf[root.children[0].ID] == nil {
			t.Errorf("Left child not written")
		}

		if bs.writeBuf[root.children[1].ID] == nil {
			t.Errorf("Right child not written")
		}
	})

	t.Run("Left internal node sibling has t keys", func(t *testing.T) {
		u := util{t}

		movedChild := makeBufPage(2, makeDocs())
		root := makeBufPage(2, makeDocs("c"),
			makeBufPage(2, makeDocs("a", "b"),
				makeBufPage(2, makeDocs()),
				makeBufPage(2, makeDocs()),
				movedChild,
			),
			makeBufPage(2, makeDocs("d"),
				makeBufPage(2, makeDocs()),
				makeBufPage(2, makeDocs()),
			),
		)

		handleSparsePage(root, root.children[1])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("b")
			nu.hasNChildren(2)
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasNChildren(3)
			nu.hasKeys("c", "d")
			if nu.node.children[0] != movedChild {
				t.Errorf("Right child expected movedChild as its first child")
			}
		})
	})

	t.Run("Right sibling has t keys", func(t *testing.T) {
		u := util{t}
		bs.flush()
		root := makeBufPage(2, makeDocs("c"),
			makeBufPage(2, makeDocs("a")),
			makeBufPage(2, makeDocs("d", "e")),
		)

		handleSparsePage(root, root.children[0])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "c")
		})

		if got := len(bs.writeBuf); got != 3 {
			t.Errorf("Got=%d, Want=3", got)
		}

		if bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}

		if bs.writeBuf[root.children[0].ID] == nil {
			t.Errorf("Left child not written")
		}

		if bs.writeBuf[root.children[1].ID] == nil {
			t.Errorf("Right child not written")
		}
	})

	t.Run("Right internal node sibling has t keys", func(t *testing.T) {
		u := util{t}
		movedChild := makePage(2, makeDocs())
		root := makePage(2, makeDocs("c"),
			makePage(2, makeDocs("a"),
				makePage(2, makeDocs()),
				makePage(2, makeDocs()),
			),
			makePage(2, makeDocs("d", "e"),
				movedChild,
				makePage(2, makeDocs()),
				makePage(2, makeDocs()),
			),
		)

		handleSparsePage(root, root.children[0])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "c")
			nu.hasNChildren(3)
			if nu.node.children[2] != movedChild {
				t.Errorf("LeftChild, expected movedChild as last child")
			}
		})
	})

	t.Run("Both siblings are sparse", func(t *testing.T) {
		u := util{t}
		bs.flush()

		mergedPage := makeBufPage(2, makeDocs("a"))
		deletedPage := makeBufPage(2, makeDocs("c"))

		root := makeBufPage(2, makeDocs("b", "d"),
			mergedPage,
			deletedPage,
			makeBufPage(2, makeDocs("e")),
		)

		handleSparsePage(root, root.children[1])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})

		if got := len(bs.writeBuf); got != 2 {
			t.Errorf("Got=%d Want=2", got)
		}

		if bs.writeBuf[root.ID] == nil {
			t.Errorf("Root not written")
		}
		if bs.writeBuf[mergedPage.ID] == nil {
			t.Errorf("Merged page not written")
		}

		if got := len(bs.deleteBuf); got != 1 {
			t.Errorf("Got=%d Want=1", got)
		}
		if bs.deleteBuf[deletedPage.ID] == nil {
			t.Errorf("Deleted page not deleted")
		}

	})

	t.Run("Both siblings are sparse; no right sibling", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeDocs("b", "d"),
			makePage(2, makeDocs("a")),
			makePage(2, makeDocs("c")),
			makePage(2, makeDocs("e")),
		)

		handleSparsePage(root, root.children[2])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d", "e")
		})
	})

	t.Run("Both siblings are sparse; no left sibling", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeDocs("b", "d"),
			makePage(2, makeDocs("a")),
			makePage(2, makeDocs("c")),
			makePage(2, makeDocs("e")),
		)

		handleSparsePage(root, root.children[0])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})
	})

	t.Run("Target key is moved from child to parent", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeDocs("b"),
			makePage(2, makeDocs("a")),
			makePage(2, makeDocs("c")),
		)

		handleSparsePage(root, root.children[1])

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNDocs(0)
			nu.hasNChildren(1)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})
	})
}
