package store

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

func newMockRepo() (repository.Repository, *ioReporter) {
	reporter := newIOReporter()
	return repository.New("", repository.NoOpCodec{}, reporter), reporter
}

func TestGet(t *testing.T) {
	repo, _ := newMockRepo()
	//u := util{t}

	var (
		root = makePage(2, makeDocs("g"),
			makePage(2, []types.Document{
				types.NewDoc("a"),
				types.NewDoc("b"),
				types.NewDoc("c").Set(map[string]interface{}{
					"value": 100,
				}),
			}),
			makePage(2, []types.Document{
				types.NewDoc("h"),
				types.NewDoc("i").Set(map[string]interface{}{
					"value": 99,
				}),
				types.NewDoc("j"),
			}),
		)
		ki = newIndex(2, repo)
	)

	fmt.Println(root, ki)

	//ki.root = root

	for _, test := range []struct {
		k    string
		want interface{}
	}{
		{"0", nil},
		{"5", nil},
		{"1000", nil},
		{"c", 100},
		{"i", 99},
	} {
		fmt.Println(test)
		//doc, err := ki.get(ctx, test.k)
		//if test.want != nil {
		//got, _ := doc.Fields["value"]
		//if !reflect.DeepEqual(got.Value, test.want) {
		//t.Errorf("Got %s, Want %s", got.Value, test.want)
		//}
		//} else if err == nil {
		//t.Errorf("Expected error but got nil")
		//}
	}
}

func TestInsert(t *testing.T) {
	//u := util{t}

	t.Run("KI is saved if root changes", func(t *testing.T) {
		//reporter := newIOReporter()
		//ki := newIndex(2)
		//ki.root.docs = makeDocs("k", "o", "s")

		//ki.insert(context.Background(), types.NewDoc("q"))
		//ki.bufWriter.flush()

		//u.with("Root", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(2)
		//})

		//if !reporter.writes["key_index"] {
		//t.Errorf("KeyIndex not written")
		//}

		//if !reporter.writes[ki.root.ID] {
		//t.Errorf("New root not written")
		//}

		//if !reporter.writes[ki.root.children[0].ID] {
		//t.Errorf("New root not written")
		//}

		//if !reporter.writes[ki.root.children[1].ID] {
		//t.Errorf("New root not written")
		//}

		//if ki.RootID != ki.root.ID {
		//t.Errorf("Want=%s Got=%s", ki.root.ID, ki.RootID)
		//}

	})

	t.Run("Insertion into tree with full root and full target leaf", func(t *testing.T) {
		var (
			root = makePage(2, makeDocs("k", "o", "s"),
				makePage(2, makeDocs("a", "b", "c")),
				makePage(2, makeDocs("l", "m")),
				makePage(2, makeDocs("p", "q")),
				makePage(2, makeDocs("x", "y")),
			)
			//ki = newIndex(2)
		)

		fmt.Println(root)
		//ki.root = root

		//var (
		//recordA = types.NewDoc("d").Set(map[string]interface{}{"value": []byte{99}})
		//recordB = types.NewDoc("r").Set(map[string]interface{}{"value": []byte{99}})
		//recordC = types.NewDoc("z").Set(map[string]interface{}{"value": []byte{99}})
		//)

		//ctx := context.Background()

		//ki.insert(ctx, recordA)
		//ki.insert(ctx, recordB)
		//ki.insert(ctx, recordC)

		//if ki.root == root {
		//t.Errorf("[TestTreeInsert]: Expected new root")
		//}

		//u.with("root", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(2)
		//nu.hasKeys("o")
		//})

		//rootSibling := ki.root.children[1]
		//u.with("Root sibling", rootSibling, func(nu namedUtil) {
		//nu.hasKeys("s")
		//nu.hasNChildren(2)
		//})

		//u.hasKeys("Root sibling, child 0", []string{"p", "q", "r"}, rootSibling.children[0])
		//u.hasKeys("Root sibling, child 0", []string{"x", "y", "z"}, rootSibling.children[1])

		//u.with("Old root", root, func(nu namedUtil) {
		//nu.hasKeys("b", "k")
		//nu.hasNChildren(3)
		//})

		//u.hasKeys("Old root, child 0", []string{"a"}, root.children[0])
		//u.hasKeys("Old root, child 1", []string{"c", "d"}, root.children[1])
		//u.hasKeys("Old root, child 2", []string{"l", "m"}, root.children[2])
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	//u := util{t}

	t.Run("KI is saved if root becomes empty", func(t *testing.T) {
		//reporter := newIOReporter()
		//ki := newIndex(2)
		//deleteMe := ki.newPage(true)

		//oldRoot := ki.newPage(false)
		//oldRoot.docs = append(oldRoot.docs, types.NewDoc("5"))
		//oldRoot.children = []*Page{
		//ki.newPage(true),
		//deleteMe,
		//}

		//oldRoot.children[0].docs = append(oldRoot.children[0].docs, types.NewDoc("2"))
		//oldRoot.children[1].docs = append(oldRoot.children[1].docs, types.NewDoc("8"))

		//ki.root = oldRoot

		//ki.remove(ctx, "5")

		//u.with("Root", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(0)
		//})

		//if len(reporter.writes) != 2 {
		//t.Errorf("Want=2 Got=%d", len(reporter.writes))
		//}

		//if !reporter.writes["key_index"] {
		//t.Errorf("KeyIndex not written")
		//}

		//if !reporter.writes[ki.root.ID] {
		//t.Errorf("New root not written")
		//}

		//if len(reporter.deletes) != 2 {
		//t.Errorf("Want=1 Got=%d", len(reporter.deletes))
		//}
		//if !reporter.deletes[oldRoot.Name] {
		//t.Errorf("Old root not deleted")
		//}
		//if !reporter.deletes[deleteMe.Name] {
		//t.Errorf("Old root not deleted")
		//}
	})

	//t.Run("Delete missing key", func(t *testing.T) {
	//ki := newIndex(2)
	//ki.root = makePage(2, makeDocs("5"))

	//err := ki.remove(ctx, "10")
	//if err == nil {
	//t.Errorf("Deleting a missing key should return an error. Got=<nil>")
	//}
	//})

	//t.Run("Delete key from tree with a single key", func(t *testing.T) {
	//u := util{t}
	//ki := newIndex(2)
	//ki.root = makePage(2, makeDocs("5"))

	//ki.remove(ctx, "5")
	//u.hasNDocs("Root", 0, ki.root)
	//u.hasNChildren("Root", 0, ki.root)
	//})

	//t.Run("Delete from root with 1 key", func(t2 *testing.T) {
	//u := util{t2}
	//ki := newIndex(2)
	//ki.root = makePage(2, makeDocs("5"),
	//makePage(2, makeDocs("2")),
	//makePage(2, makeDocs("8")),
	//)

	//ki.remove(ctx, "5")

	//u.with("Root", ki.root, func(nu namedUtil) {
	//nu.hasKeys("2", "8")
	//nu.hasNChildren(0)
	//})
	//})

	// Case 1: Delete From Leaf
	t.Run("Delete from leaf", func(t *testing.T) {
		repo, _ := newMockRepo()
		ki := newIndex(2, repo)
		root := makePage(2, makeDocs("1", "2", "3"))
		//ki.root = root

		ki.remove(ctx, "2")

		u := util{t, repo}
		u.with("leaf", root.ID(), func(nu namedUtil) {
			nu.hasNDocs(2)
			nu.hasKeys("1", "3")
			nu.hasNChildren(0)
		})
	})
}

func BenchmarkInsertKeyIndex(b *testing.B) {
	for _, t := range []int{2, 20, 50, 100, 200, 500, 1000, 2000} {
		b.Run(fmt.Sprintf("t=%d, b.N=%d", t, b.N), func(b *testing.B) {
			//ki := newIndex(t)
			//ctx := context.Background()

			for i := 0; i < b.N; i++ {
				//ki.insert(ctx, types.NewDoc(fmt.Sprint(i)))
			}
		})
	}
}

func TestBuildKeyIndex(t *testing.T) {
	t.Run("Short", func(t *testing.T) {
		//u := util{t}

		//ki := newIndex(2)
		//ctx := context.Background()

		var (
		//recordA = types.NewDoc("a")
		//recordB = types.NewDoc("b")
		//recordC = types.NewDoc("c")

		//recordD = types.NewDoc("d")
		//recordE = types.NewDoc("e")
		)

		//ki.insert(ctx, recordA)
		//ki.insert(ctx, recordB)
		//ki.insert(ctx, recordC)

		//u.with("Root after 3 insertions, t=2", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(0)
		//nu.hasKeys("a", "b", "c")
		//})

		//ki.insert(ctx, recordD)
		//ki.insert(ctx, recordE)

		//u.with("Root after 5 insertions", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(2)
		//nu.hasKeys("b")
		//})

		//u.with("Left child after 5 insertions", ki.root.children[0], func(nu namedUtil) {
		//nu.hasNChildren(0)
		//nu.hasKeys("a")
		//})

		//u.with("Right child after 5 insertions", ki.root.children[1], func(nu namedUtil) {
		//nu.hasNChildren(0)
		//nu.hasKeys("c", "d", "e")
		//})

		//ki.remove(ctx, "e")
		//ki.remove(ctx, "d")
		//ki.remove(ctx, "c")

		//u.with("Root after deleting 3 times", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(0)
		//nu.hasKeys("a", "b")
		//})

		//ki.remove(ctx, "a")
		//ki.remove(ctx, "b")

		//u.with("Root should be empty", ki.root, func(nu namedUtil) {
		//nu.hasNChildren(0)
		//nu.hasNDocs(0)
		//})
	})
}

func equal(a, b interface{}) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
