package store

import (
	"fmt"
	"testing"
)

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
		root := newPage(100)
		root.records = makeNewRecords(test.keys)
		gotIndex, gotExists := root.keyIndex(test.k)

		if gotIndex != test.wantIndex || gotExists != test.wantExists {
			t.Errorf("[TestKeyIndex] %d: Got (%v, %v); Want (%v, %v)", i, gotIndex, gotExists, test.wantIndex, test.wantExists)
		}
	}
}

func TestInsertRecord(t *testing.T) {
	u := util{t}
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
		r := newPage(3)
		r.leaf = true
		r.records = makeNewRecords(test.keys)

		r.insert(test.k, nil)

		u.hasKeys(fmt.Sprintf("TestLeafInsert %d", i), test.wantKeys, r)
	}
}

func TestSplitChild(t *testing.T) {
	u := util{t}
	t.Run("Full leaf child", func(t *testing.T) {
		var (
			root = makePage(2, makeRecords("10"),
				makePage(2, makeRecords("1", "4", "8")),
				makePage(2, makeRecords("12", "14", "20")),
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
		l2a_child := makePage(2, makeRecords("6", "7"))
		l2b_child := makePage(2, makeRecords("9", "10"))
		l2c_child := makePage(2, makeRecords("16", "17"))
		l2d_child := makePage(2, makeRecords("19", "20"))
		root := makePage(2, makeRecords("21"),
			makePage(2, makeRecords("8", "15", "18"),
				l2a_child,
				l2b_child,
				l2c_child,
				l2d_child,
			),
			makePage(2, makeRecords()),
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
			root     = makePage(2, makeRecords("5"),
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

		root.children = []*Page{childA, childC}
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

		root.children = []*Page{childA, childC}
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
		u := util{t}
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
		u := util{t}
		var (
			childA = newNodeWithKeys(2, []string{"2"})
			childB = newNodeWithKeys(2, []string{"2"})
			childC = newNodeWithKeys(2, []string{"8"})

			newChildA = newNodeWithKeys(2, []string{"8"})
			newChildB = newNodeWithKeys(2, []string{"8"})

			root = makePage(2, makeRecords("5"),
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
		err := node.Delete("b")
		if err == nil {
			t.Errorf("deleteKey should return error if key is not found")
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
			var (
				node = makePage(2, makeRecords("a", "b", "c"))
				err  = node.Delete(test.targetKey)
			)

			if err != nil {
				t.Errorf("Should not return error")
			}

			u.hasKeys(fmt.Sprintf("[DeleteKey]: %d", index), test.want, node)
		}
	})

	t.Run("Internal node, predecessor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makePage(2, makeRecords("5"),
			makePage(2, makeRecords("2", "3")),
			makePage(2, makeRecords("6")),
		)

		root.Delete("5")

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

	t.Run("Deep internal node, predecessor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makePage(2, makeRecords("9"),
			makePage(2, makeRecords("3", "5"),
				makePage(2, makeRecords("2")),
				makePage(2, makeRecords("4")),
				makePage(2, makeRecords("6")),
			),
			makePage(2, makeRecords("10000")),
		)

		root.Delete("9")

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
	})

	t.Run("Internal node, successor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makePage(2, makeRecords("5"),
			makePage(2, makeRecords("2")),
			makePage(2, makeRecords("6", "7")),
		)

		root.Delete("5")

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

	t.Run("Deep internal node, successor has t keys", func(t *testing.T) {
		u := &util{t}
		root := makePage(2, makeRecords("3"),
			makePage(2, makeRecords("10000")),
			makePage(2, makeRecords("5", "8"),
				makePage(2, makeRecords("4")),
				makePage(2, makeRecords("7")),
				makePage(2, makeRecords("9")),
			),
		)

		root.Delete("3")

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
	})

	t.Run("Internal node, predecessor and successor have t-1 keys", func(t *testing.T) {
		u := &util{t}
		root := makePage(2, makeRecords("5"),
			makePage(2, makeRecords("2")),
			makePage(2, makeRecords("6")),
		)

		root.Delete("5")

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(0)
			nu.hasNChildren(1)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("2", "6")
				nu.hasNChildren(0)
			})
		})

	})
}

func TestMergeChildren(t *testing.T) {
	u := util{t}
	root := makePage(2, makeRecords("5", "10", "15"),
		makePage(2, makeRecords("2"),
			makePage(2, makeRecords()),
			makePage(2, makeRecords()),
		),
		makePage(2, makeRecords("7", "8"),
			makePage(2, makeRecords()),
			makePage(2, makeRecords()),
			makePage(2, makeRecords()),
		),
		makePage(2, makeRecords("11"),
			makePage(2, makeRecords()),
			makePage(2, makeRecords()),
		),
		makePage(2, makeRecords("16"),
			makePage(2, makeRecords()),
			makePage(2, makeRecords()),
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

func TestPredecessorSuccessorKeyNode(t *testing.T) {
	target := makePage(2, makeRecords("99"))
	root := makePage(2, makeRecords("a", "c"),
		makePage(2, makeRecords()),
		target,
		makePage(2, makeRecords()),
	)

	if root.predecessorNode("c") != target {
		t.Errorf("%v", root)
	}

	if root.predecessorNode("c") != root.successorNode("a") {
		t.Errorf("root.predecessorKeyNode(index) should be root.successorKeyNode(index-1)")
	}
}

func TestFull(t *testing.T) {
	root := newPage(2)

	if got := root.Full(); got {
		t.Errorf("New(2).IsFull() = %v; want false", got)
	}

	root.records = makeNewRecords([]string{"1", "2", "3"})

	if got := root.Full(); !got {
		t.Errorf("Want root.IsFull() = true, got %v", got)
	}
}

func TestIsSparse(t *testing.T) {
	for i, test := range []struct {
		t          int
		keys       []string
		wantSparse bool
	}{
		{3, []string{"1"}, true},
		{3, []string{"1", "2"}, true},
		{3, []string{"1", "2", "3"}, false},
	} {
		node := newNodeWithKeys(test.t, test.keys)

		if got := node.Sparse(); got != test.wantSparse {
			t.Errorf("%d: Got=%v; Want=%v", i, got, test.wantSparse)
		}

	}
}

func TestChildSibling(t *testing.T) {
	var (
		child   = makePage(2, makeRecords())
		sibling = makePage(2, makeRecords())
		root    = makePage(2, makeRecords("c", "e", "f"),
			makePage(2, makeRecords()),
			child,
			sibling,
			makePage(2, makeRecords()),
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
		root := makePage(2, makeRecords("3"), makePage(2, makeRecords("a")), makePage(2, makeRecords("5", "7", "9")))

		modified := splitFullPage(root, root.children[1])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

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
		root := makePage(2, makeRecords("9"), makePage(2, makeRecords("3", "5", "8")), makePage(2, makeRecords("a")))

		modified := splitFullPage(root, root.children[0])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

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
		root := makePage(2, makeRecords("9"), makePage(2, makeRecords("3", "8")), makePage(2, makeRecords("a")))

		modified := splitFullPage(root, root.children[0])
		if modified {
			t.Errorf("Want=%v, Got=%v", false, modified)
		}

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
	t.Run("Left sibling has t keys", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeRecords("c"),
			makePage(2, makeRecords("a", "b")),
			makePage(2, makeRecords("d")),
		)

		modified := handleSparsePage(root, root.children[1])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasKeys("b")
			nu.hasNChildren(2)
		})

		u.with("Right child", root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d")
		})
	})

	t.Run("Left internal node sibling has t keys", func(t *testing.T) {
		u := util{t}
		movedChild := makePage(2, makeRecords())
		root := makePage(2, makeRecords("c"),
			makePage(2, makeRecords("a", "b"),
				makePage(2, makeRecords()),
				makePage(2, makeRecords()),
				movedChild,
			),
			makePage(2, makeRecords("d"),
				makePage(2, makeRecords()),
				makePage(2, makeRecords()),
			),
		)

		modified := handleSparsePage(root, root.children[1])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
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
		root := makePage(2, makeRecords("c"),
			makePage(2, makeRecords("a")),
			makePage(2, makeRecords("d", "e")),
		)

		modified := handleSparsePage(root, root.children[0])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)
		})

		u.with("Left child", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "c")
		})
	})

	t.Run("Right internal node sibling has t keys", func(t *testing.T) {
		u := util{t}
		movedChild := makePage(2, makeRecords())
		root := makePage(2, makeRecords("c"),
			makePage(2, makeRecords("a"),
				makePage(2, makeRecords()),
				makePage(2, makeRecords()),
			),
			makePage(2, makeRecords("d", "e"),
				movedChild,
				makePage(2, makeRecords()),
				makePage(2, makeRecords()),
			),
		)

		modified := handleSparsePage(root, root.children[0])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
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
		root := makePage(2, makeRecords("b", "d"),
			makePage(2, makeRecords("a")),
			makePage(2, makeRecords("c")),
			makePage(2, makeRecords("e")),
		)

		modified := handleSparsePage(root, root.children[1])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})
	})

	t.Run("Both siblings are sparse; no right sibling", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeRecords("b", "d"),
			makePage(2, makeRecords("a")),
			makePage(2, makeRecords("c")),
			makePage(2, makeRecords("e")),
		)

		modified := handleSparsePage(root, root.children[2])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[1], func(nu namedUtil) {
			nu.hasKeys("c", "d", "e")
		})
	})

	t.Run("Both siblings are sparse; no left sibling", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeRecords("b", "d"),
			makePage(2, makeRecords("a")),
			makePage(2, makeRecords("c")),
			makePage(2, makeRecords("e")),
		)

		modified := handleSparsePage(root, root.children[0])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(1)
			nu.hasNChildren(2)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})
	})

	t.Run("Target key is moved from child to parent", func(t *testing.T) {
		u := util{t}
		root := makePage(2, makeRecords("b"),
			makePage(2, makeRecords("a")),
			makePage(2, makeRecords("c")),
		)

		modified := handleSparsePage(root, root.children[1])
		if !modified {
			t.Errorf("Want=%v, Got=%v", true, modified)
		}

		u.with("Root", root, func(nu namedUtil) {
			nu.hasNRecords(0)
			nu.hasNChildren(1)
		})

		u.with("Merged node", root.children[0], func(nu namedUtil) {
			nu.hasKeys("a", "b", "c")
		})
	})
}
