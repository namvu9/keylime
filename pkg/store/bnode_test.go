package store

import (
	"testing"
)

func TestIsFull(t *testing.T) {
	root := newNode(2)

	if got := root.isFull(); got {
		t.Errorf("New(2).IsFull() = %v; want false", got)
	}

	root.records = makeNewKeys([]string{"1", "2", "3"})

	if got := root.isFull(); !got {
		t.Errorf("Want root.IsFull() = true, got %v", got)
	}
}

func TestKeyIndex(t *testing.T) {
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
	} {
		root := newNode(100)
		root.records = makeNewKeys(test.keys)
		gotIndex, gotExists := root.keyIndex(test.k)

		if gotIndex != test.wantIndex || gotExists != test.wantExists {
			t.Errorf("[TestKeyIndex] %d: Got (%v, %v); Want (%v, %v)", i, gotIndex, gotExists, test.wantIndex, test.wantExists)
		}
	}
}

func TestLeafInsert(t *testing.T) {
	for i, test := range []struct {
		k        string
		keys     []string
		wantKeys []string
	}{
		{"2", []string{"1", "3", "5"}, []string{"1", "2", "3", "5"}},
		{"0", []string{"1", "3", "5"}, []string{"0", "1", "3", "5"}},
		{"4", []string{"1", "3", "5"}, []string{"1", "3", "4", "5"}},
		{"6", []string{"1", "3", "5"}, []string{"1", "3", "5", "6"}},
	} {
		r := newNode(3)
		r.leaf = true
		r.records = makeNewKeys(test.keys)
		r.insert(test.k, nil)

		want := makeNewKeys(test.wantKeys)

		if !r.records.equals(want) {
			t.Errorf("[TestLeafInsert] %d: Want=%v, Got=%v", i, test.wantKeys, r.records.keys())
		}
	}
}

func TestSplitChild(t *testing.T) {
	u := util{t}
	t.Run("Full leaf child", func(t *testing.T) {
		var (
			root = makeTree(2, makeRecords("10"),
				makeTree(2, makeRecords("1", "4", "8")),
				makeTree(2, makeRecords("12", "14", "20")),
			)
		)

		root.splitChild(1)

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("10", "14")
			nu.hasNChildren(3)
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasKeys("12")
		})

		u.with("New child", root.children[2], func(nu namedUtil) {
			nu.hasKeys("20")
		})
	})

	t.Run("Full internal node", func(t *testing.T) {
		l2a_child := makeTree(2, makeRecords("6", "7"))
		l2b_child := makeTree(2, makeRecords("9", "10"))
		l2c_child := makeTree(2, makeRecords("16", "17"))
		l2d_child := makeTree(2, makeRecords("19", "20"))
		root := makeTree(2, makeRecords("21"),
			makeTree(2, makeRecords("8", "15", "18"),
				l2a_child,
				l2b_child,
				l2c_child,
				l2d_child,
			),
			makeTree(2, makeRecords()),
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
	})
}

func TestInsertChild(t *testing.T) {
	u := util{t}
	t.Run("Prepend", func(t *testing.T) {
		var (
			childA   = newNodeWithKeys(2, []string{"2"})
			childC   = newNodeWithKeys(2, []string{"8"})
			newChild = newNodeWithKeys(2, []string{"10"})
			root     = makeTree(2, makeRecords("5"),
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
			root     = newNodeWithKeys(2, []string{"5"})
			childA   = newNodeWithKeys(2, []string{"2"})
			childC   = newNodeWithKeys(2, []string{"8"})
			newChild = newNodeWithKeys(2, []string{"10"})
		)

		root.children = []*BNode{childA, childC}
		root.insertChildren(1, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, newChild, childC)
		})
	})

	t.Run("Append", func(t *testing.T) {
		var (
			root     = newNodeWithKeys(2, []string{"5"})
			childA   = newNodeWithKeys(2, []string{"2"})
			childC   = newNodeWithKeys(2, []string{"8"})
			newChild = newNodeWithKeys(2, []string{"10"})
		)

		root.children = []*BNode{childA, childC}
		root.insertChildren(2, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, childC, newChild)
		})
	})

	t.Run("Empty", func(t *testing.T) {
		var (
			root     = newNodeWithKeys(2, []string{"5"})
			newChild = newNodeWithKeys(2, []string{"10"})
		)

		root.insertChildren(0, newChild)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(newChild)
		})
	})

	t.Run("Multiple into empty", func(t *testing.T) {
		var (
			root   = newNodeWithKeys(2, []string{"5"})
			childA = newNodeWithKeys(2, []string{"2"})
			childB = newNodeWithKeys(2, []string{"2"})
			childC = newNodeWithKeys(2, []string{"8"})
		)

		root.insertChildren(0, childA, childB, childC)
		u.with("Root", root, func(nu namedUtil) {
			nu.hasChildren(childA, childB, childC)
		})
	})

	t.Run("Insert multiple", func(t *testing.T) {
		var (
			childA = newNodeWithKeys(2, []string{"2"})
			childB = newNodeWithKeys(2, []string{"2"})
			childC = newNodeWithKeys(2, []string{"8"})

			newChildA = newNodeWithKeys(2, []string{"8"})
			newChildB = newNodeWithKeys(2, []string{"8"})

			root = makeTree(2, makeRecords("5"),
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

func TestDeleteNode(t *testing.T) {
	t.Run("Missing key", func(t *testing.T) {
		node := newNodeWithKeys(2, []string{"a", "c"})
		err := node.deleteKey("b")
		if err == nil {
			t.Errorf("deleteKey should return error if key is not found")
		}
	})

	t.Run("Delete existing key in leaf", func(t *testing.T) {
		for index, test := range []struct {
			targetKey string
			want      []string
		}{
			{"a", []string{"b", "c"}},
			{"b", []string{"a", "c"}},
			{"c", []string{"a", "b"}},
		} {
			node := newNodeWithKeys(2, []string{"a", "b", "c"})
			node.leaf = true
			err := node.deleteKey(test.targetKey)

			if err != nil {
				t.Errorf("Should not return error")
			}

			if !node.records.contains(test.want) {
				t.Errorf("[DeleteKey] %d: Got=%v, Want=%v", index, node.records.keys(), test.want)
			}
		}
	})

	t.Run("Internal node, predecessor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makeTree(2, makeRecords("5"),
			makeTree(2, makeRecords("2", "3")),
			makeTree(2, makeRecords("6")),
		)

		root.deleteKey("5")

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
	})

	t.Run("Internal node, successor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makeTree(2, makeRecords("5"),
			makeTree(2, makeRecords("2")),
			makeTree(2, makeRecords("6", "7")),
		)

		root.deleteKey("5")

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
	})

	t.Run("Internal node, predecessor and successor have t-1 keys", func(t *testing.T) {
		u := &util{t}
		root := makeTree(2, makeRecords("5"),
			makeTree(2, makeRecords("2")),
			makeTree(2, makeRecords("6")),
		)

		root.deleteKey("5")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(0)
			nu.hasNChildren(1)
		})

	})
}

func TestMergeChildren(t *testing.T) {
	u := util{t}
	root := makeTree(2, makeRecords("5", "10", "15"),
		makeTree(2, makeRecords("2"),
			makeTree(2, makeRecords()),
			makeTree(2, makeRecords()),
		),
		makeTree(2, makeRecords("7", "8"),
			makeTree(2, makeRecords()),
			makeTree(2, makeRecords()),
			makeTree(2, makeRecords()),
		),
		makeTree(2, makeRecords("11"),
			makeTree(2, makeRecords()),
			makeTree(2, makeRecords()),
		),
		makeTree(2, makeRecords("16"),
			makeTree(2, makeRecords()),
			makeTree(2, makeRecords()),
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
