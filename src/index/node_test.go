package index

import (
	"fmt"
	"testing"
)

func TestDeleteFromNode(t *testing.T) {
	t.Run("Missing key", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)

		root := makeTree(MakeRecords("a", "c"))

		err := root.remove("b")
		if err == nil {
			t.Errorf("deleteKey should return error if key is not found")
		}

		repo.Flush()

		if len(reporter.Writes) != 0 {
			t.Errorf("Failed deletion should not schedule any writes")
		}
	})

	t.Run("Delete key in leaf", func(t *testing.T) {
		for i, test := range []struct {
			targetKey string
			want      []string
		}{
			{"a", []string{"b", "c"}},
			{"b", []string{"a", "c"}},
			{"c", []string{"a", "b"}},
		} {
			repo, reporter := newMockRepo(2)
			makeTree := TreeFactory(repo)
			u := util{t, repo}

			node := makeTree(MakeRecords("a", "b", "c"))

			err := node.remove(test.targetKey)
			if err != nil {
				t.Errorf("Should not return error")
			}

			repo.Flush()

			u.with("Node", node.ID(), func(nu namedUtil) {
				nu.hasKeys(test.want...)
			})

			if got := len(reporter.Writes); got != 1 {
				t.Errorf("Deleting from leaf: Want=%d, Got=%d", 1, got)
			}

			if _, ok := reporter.Writes[node.ID()]; !ok {
				t.Errorf("%d: Node was not written", i)
			}
		}
	})

	t.Run("Internal node, predecessor has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		root := makeTree(MakeRecords("5"),
			makeTree(MakeRecords("2", "3")),
			makeTree(MakeRecords("6")),
		)

		root.remove("5")
		repo.Flush()

		if got := len(reporter.Writes); got != 2 {
			t.Errorf("Buffered Writes: Got=%d, Want=%d", got, 2)
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("3")
			nu.hasNChildren(2)

			if _, ok := reporter.Writes[nu.node.ID()]; !ok {
				t.Errorf("root.children[0] not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("2")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("root.children[0] not written")
				}
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("6")
			})
		})

	})

	t.Run("Deep internal node, predecessor has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		leafA := makeTree(MakeRecords("2"))
		leafB := makeTree(MakeRecords("4"))
		leafC := makeTree(MakeRecords("6"))

		root := makeTree(MakeRecords("9"),
			makeTree(MakeRecords("3", "5"),
				leafA,
				leafB,
				leafC,
			),
			makeTree(MakeRecords("10000")),
		)

		root.remove("9")
		repo.Flush()

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("6")
			nu.hasNChildren(2)

			if _, ok := reporter.Writes[nu.node.ID()]; !ok {
				t.Errorf("Left node not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasNChildren(2)
				nu.hasKeys("3")

				nu.withChild(0, func(nu namedUtil) {
					nu.hasKeys("2")
				})
				nu.withChild(1, func(nu namedUtil) {
					nu.hasKeys("4", "5")
				})
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("10000")
			})
		})

		if got := len(reporter.Writes); got != 3 {
			t.Errorf("Buffered writes: Got=%d, Want=%d", got, 3)
		}

		if _, ok := reporter.Writes[root.ID()]; !ok {
			t.Errorf("Root not written")
		}

		if _, ok := reporter.Writes[leafB.ID()]; !ok {
			t.Errorf("Leaf B not written")
		}

		if _, ok := reporter.Deletes[leafC.ID()]; !ok {
			t.Errorf("Leaf C not deleted")
		}
	})

	t.Run("Internal node, successor has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		root := makeTree(MakeRecords("5"),
			makeTree(MakeRecords("2")),
			makeTree(MakeRecords("6", "7")),
		)

		root.remove("5")
		repo.Flush()

		if got := len(reporter.Writes); got != 2 {
			t.Errorf("Buffered Writes: Got=%d, Want=%d", got, 2)
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("6")
			nu.hasNChildren(2)

			if _, ok := reporter.Writes[root.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("2")

			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("7")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("root.children[1] not written")
				}
			})
		})
	})

	t.Run("Deep internal node, successor has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		mergedNode := makeTree(MakeRecords("4"))
		deleteNode := makeTree(MakeRecords("7"))

		root := makeTree(MakeRecords("3"),
			makeTree(MakeRecords("10000")),
			makeTree(MakeRecords("5", "8"),
				mergedNode,
				deleteNode,
				makeTree(MakeRecords("9")),
			),
		)

		root.remove("3")
		repo.Flush()

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("4")
			nu.hasNChildren(2)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("10000")
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(2)
				nu.hasKeys("8")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Right child not written")
				}

				nu.withChild(0, func(nu namedUtil) {
					nu.hasKeys("5", "7")
				})
				nu.withChild(1, func(nu namedUtil) {
					nu.hasKeys("9")
				})
			})
		})

		if got := len(reporter.Writes); got != 3 {
			t.Errorf("Buffered writes: Got=%d, Want=%d", got, 3)
		}

		if _, ok := reporter.Writes[root.ID()]; !ok {
			t.Errorf("Root not written")
		}

		if _, ok := reporter.Writes[mergedNode.ID()]; !ok {
			t.Errorf("MergedNode not written")
		}
		if _, ok := reporter.Deletes[deleteNode.ID()]; !ok {
			t.Errorf("PredNode not deleted")
		}

	})

	t.Run("Internal node, predecessor and successor have t-1 keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		deletedNode := makeTree(MakeRecords("6"))

		root := makeTree(MakeRecords("5"),
			makeTree(MakeRecords("2")),
			deletedNode,
		)

		root.remove("5")
		repo.Flush()

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(0)
			nu.hasNChildren(1)

			if _, ok := reporter.Writes[nu.node.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("2", "6")
				nu.hasNChildren(0)

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Left child not written")
				}
			})
		})

		if len(reporter.Writes) != 2 {
			t.Errorf("Want=%d Got=%d", 2, len(reporter.Writes))
		}

		if len(reporter.Deletes) != 1 {
			t.Errorf("Want=%d Got=%d", 1, len(reporter.Deletes))
		}

		if _, ok := reporter.Deletes[deletedNode.ID()]; !ok {
			t.Errorf("Right child not deleted")
		}
	})
}

func TestInsertRecord(t *testing.T) {
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
		repo, reporter := newMockRepo(3)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		root := makeTree(MakeRecords(test.keys...))

		root.insert(Record{Key: test.k})
		repo.Flush()

		u.hasKeys(fmt.Sprintf("TestLeafInsert %d", i), test.wantKeys, root)

		if want, got := 1, len(reporter.Writes); want != got {
			t.Errorf("Writes, want=%d got=%d", want, got)
		}

		if _, ok := reporter.Writes[root.ID()]; !ok {
			t.Errorf("Root not written")
		}

	}
}

func TestSplitChild(t *testing.T) {
	t.Run("Full leaf child", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)

		u := util{t, repo}

		root := makeTree(MakeRecords("10"),
			makeTree(MakeRecords("1", "4", "8")),
			makeTree(MakeRecords("12", "14", "20")),
		)

		root.splitChild(1)
		repo.Flush()

		if len(reporter.Writes) != 3 {
			t.Errorf("Want=%d Got=%d", 3, len(reporter.Writes))
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("10", "14")
			nu.hasNChildren(3)

			if _, ok := reporter.Writes[root.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("12")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Full child not written")
				}

			})

			nu.withChild(2, func(nu namedUtil) {
				nu.hasKeys("20")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("New child not written")
				}
			})
		})

	})

	t.Run("Full internal node", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		l2aChild := makeTree(MakeRecords())
		l2bChild := makeTree(MakeRecords())
		l2cChild := makeTree(MakeRecords())
		l2dChild := makeTree(MakeRecords())

		root := makeTree(MakeRecords("21"),
			makeTree(MakeRecords("8", "15", "18"),
				l2aChild,
				l2bChild,
				l2cChild,
				l2dChild,
			),
			makeTree(MakeRecords()),
		)

		root.splitChild(0)
		repo.Flush()

		if got := len(reporter.Writes); got != 3 {
			t.Errorf("Got=%d Want=3", got)
		}

		if got := len(reporter.Deletes); got != 0 {
			t.Errorf("Got=%d Want=3", got)
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("15", "21")
			nu.hasNChildren(3)

			if _, ok := reporter.Writes[root.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasChildren(l2aChild, l2bChild)
				nu.hasKeys("8")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Full child not written")
				}
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("18")
				nu.hasChildren(l2cChild, l2dChild)

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("New child not written")
				}
			})
		})

	})

	t.Run("Full leaf child 2", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		root := makeTree(MakeRecords("1", "3"),
			makeTree(MakeRecords("0")),
			makeTree(MakeRecords("2")),
			makeTree(MakeRecords("4", "5", "6")),
		)

		root.splitChild(2)
		repo.Flush()

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("1", "3", "5")
			nu.hasNChildren(4)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("0")
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("2")
			})

			nu.withChild(2, func(nu namedUtil) {
				nu.hasNChildren(0)
				nu.hasKeys("4")
			})

			nu.withChild(3, func(nu namedUtil) {
				nu.hasKeys("6")
				nu.hasNChildren(0)
			})
		})
	})
}

func TestMergeChildren(t *testing.T) {
	repo, _ := newMockRepo(2)
	makeTree := TreeFactory(repo)

	u := util{t, repo}

	root := makeTree(MakeRecords("5", "10", "15"),
		makeTree(MakeRecords("2"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		),
		makeTree(MakeRecords("7", "8"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		),
		makeTree(MakeRecords("11"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		),
		makeTree(MakeRecords("16"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		),
	)

	root.mergeChildren(1)

	u.with("Root", root.ID(), func(nu namedUtil) {
		nu.hasKeys("5", "15")
		nu.hasNChildren(3)

		nu.withChild(0, func(nu namedUtil) {
			nu.hasKeys("2")
			nu.hasNChildren(2)
		})

		nu.withChild(1, func(nu namedUtil) {
			nu.hasKeys("7", "8", "10", "11")
			nu.hasNChildren(5)
		})

		nu.withChild(2, func(nu namedUtil) {
			nu.hasKeys("16")
			nu.hasNChildren(2)
		})
	})
}

func TestPredecessorSuccessorPage(t *testing.T) {
	repo, _ := newMockRepo(2)
	makeTree := TreeFactory(repo)

	target := makeTree(MakeRecords("99"))
	root := makeTree(MakeRecords("a", "c"),
		makeTree(MakeRecords()),
		target,
		makeTree(MakeRecords()),
	)

	if pred, _ := root.predecessorNode("c"); pred != target {
		t.Errorf("%v", root)
	}

	pred, _ := root.predecessorNode("c")
	succ, _ := root.successorNode("a")
	if pred != succ {
		t.Errorf("root.predecessorKeyNode(index) should be root.successorKeyNode(index-1)")
	}
}

func TestChildSibling(t *testing.T) {
	repo, _ := newMockRepo(2)
	makeTree := TreeFactory(repo)

	var (
		child   = makeTree(MakeRecords())
		sibling = makeTree(MakeRecords())
		root    = makeTree(MakeRecords("c", "e", "f"),
			makeTree(MakeRecords()),
			child,
			sibling,
			makeTree(MakeRecords()),
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

	t.Run("1", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		u := util{t, repo}

		makeTree := TreeFactory(repo)
		splitChild := makeTree(MakeRecords("5", "7", "9"))

		root := makeTree(MakeRecords("3"), makeTree(MakeRecords("a")), splitChild)

		splitFullNode(root, splitChild)

		u.with("1", root.ID(), func(nu namedUtil) {
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
		repo, _ := newMockRepo(2)
		u := util{t, repo}

		makeTree := TreeFactory(repo)
		splitChild := makeTree(MakeRecords("3", "5", "8"))
		root := makeTree(MakeRecords("9"),
			splitChild,
			makeTree(MakeRecords("a")))

		splitFullNode(root, splitChild)

		u.with("2", root.ID(), func(nu namedUtil) {
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

	t.Run("Ignore non-full child", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		u := util{t, repo}

		makeTree := TreeFactory(repo)
		splitChild := makeTree(MakeRecords("3", "8"))
		root := makeTree(MakeRecords("9"),
			splitChild,
			makeTree(MakeRecords("a")),
		)

		splitFullNode(root, splitChild)

		u.with("3", root.ID(), func(nu namedUtil) {
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

func TestHandleSparseNode(t *testing.T) {
	t.Run("Left sibling has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		rightChild := makeTree(MakeRecords("d"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		)

		root := makeTree(MakeRecords("c"),
			makeTree(MakeRecords("a", "b")),
			rightChild,
		)

		handleSparseNode(root, rightChild)
		repo.Flush()

		if got := len(reporter.Writes); got != 3 {
			t.Errorf("Got=%d, Want=3", got)
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasKeys("b")
			nu.hasNChildren(2)

			if _, ok := reporter.Writes[root.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Left child not written")
				}

			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("c", "d")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Right child not written")
				}
			})
		})
	})

	t.Run("Left internal node sibling has t keys", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		movedChild := makeTree(MakeRecords())
		rightChild := makeTree(MakeRecords("d"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		)

		root := makeTree(MakeRecords("c"),
			makeTree(MakeRecords("a", "b"),
				makeTree(MakeRecords()),
				makeTree(MakeRecords()),
				movedChild,
			),
			rightChild,
		)

		handleSparseNode(root, rightChild)

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("b")
			nu.hasNChildren(2)

			nu.withChild(1, func(nu namedUtil) {
				nu.hasNChildren(3)
				nu.hasKeys("c", "d")
				nu.withChild(0, func(nu namedUtil) {
					nu.is(movedChild)
				})
			})
		})
	})

	t.Run("Right sibling has t keys", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		leftChild := makeTree(MakeRecords("a"))

		root := makeTree(MakeRecords("c"),
			leftChild,
			makeTree(MakeRecords("d", "e")),
		)

		handleSparseNode(root, leftChild)
		repo.Flush()

		if got := len(reporter.Writes); got != 3 {
			t.Errorf("Got=%d, Want=3", got)
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)

			if _, ok := reporter.Writes[root.ID()]; !ok {
				t.Errorf("Root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a", "c")

				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Left child not written")
				}
			})

			nu.withChild(1, func(nu namedUtil) {
				if _, ok := reporter.Writes[nu.node.ID()]; !ok {
					t.Errorf("Right child not written")
				}
			})
		})

	})

	t.Run("Right internal node sibling has t keys", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		leftChild := makeTree(MakeRecords("a"),
			makeTree(MakeRecords()),
			makeTree(MakeRecords()),
		)

		movedChild := makeTree(MakeRecords())

		root := makeTree(MakeRecords("c"),
			leftChild,
			makeTree(MakeRecords("d", "e"),
				movedChild,
				makeTree(MakeRecords()),
				makeTree(MakeRecords()),
			),
		)

		handleSparseNode(root, leftChild)

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasKeys("d")
			nu.hasNChildren(2)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a", "c")
				nu.hasNChildren(3)

				nu.withChild(2, func(nu namedUtil) {
					nu.is(movedChild)
				})
			})
		})
	})

	t.Run("Both siblings are sparse", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		mergedNode := makeTree(MakeRecords("a"))
		deletedNode := makeTree(MakeRecords("c"))

		root := makeTree(MakeRecords("b", "d"),
			mergedNode,
			deletedNode,
			makeTree(MakeRecords()),
		)

		handleSparseNode(root, deletedNode)
		repo.Flush()

		if got := len(reporter.Writes); got != 2 {
			t.Errorf("Got=%d Want=2", got)
		}

		if _, ok := reporter.Writes[root.ID()]; !ok {
			t.Errorf("Root not written")
		}

		if _, ok := reporter.Writes[mergedNode.ID()]; !ok {
			t.Errorf("Merged page not written")
		}

		if got := len(reporter.Deletes); got != 1 {
			t.Errorf("Got=%d Want=1", got)
		}

		if _, ok := reporter.Deletes[deletedNode.ID()]; !ok {
			t.Errorf("Deleted page not deleted")
		}

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a", "b", "c")
			})
		})

	})

	t.Run("Both siblings are sparse; no right sibling", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		rightChild := makeTree(MakeRecords("e"))

		root := makeTree(MakeRecords("b", "d"),
			makeTree(MakeRecords("a")),
			makeTree(MakeRecords("c")),
			rightChild,
		)

		handleSparseNode(root, rightChild)

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)

			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("c", "d", "e")
			})
		})
	})

	t.Run("Both siblings are sparse; no left sibling", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		leftChild := makeTree(MakeRecords("a"))

		root := makeTree(MakeRecords("b", "d"),
			leftChild,
			makeTree(MakeRecords("c")),
			makeTree(MakeRecords("e")),
		)

		handleSparseNode(root, leftChild)

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(1)
			nu.hasNChildren(2)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a", "b", "c")
			})
		})
	})

	t.Run("Target key is moved from child to parent", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		makeTree := TreeFactory(repo)
		u := util{t, repo}

		rightChild := makeTree(MakeRecords("c"))

		root := makeTree(MakeRecords("b"),
			makeTree(MakeRecords("a")),
			rightChild,
		)

		handleSparseNode(root, rightChild)

		u.with("Root", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(0)
			nu.hasNChildren(1)

			nu.withChild(0, func(nu namedUtil) {
				nu.hasKeys("a", "b", "c")
			})
		})
	})
}
