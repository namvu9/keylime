package index

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/namvu9/keylime/src/repository"
)

func newMockRepo(t int) (repository.Repository, *repository.IOReporter) {
	repo, reporter := repository.NewMockRepo()

	return repository.WithFactory(repo, NodeFactory{t: t, repo: repo}), reporter
}

func TestMain(m *testing.M) {
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestGet(t *testing.T) {
	repo, _ := repository.NewMockRepo()

	index := New(2, repo)

	root, _ := index.New(false)
	childA, _ := index.New(true)
	childA.Records = []Record{
		{Key: "a"},
		{Key: "b"},
		{Key: "c"},
	}

	childB, _ := index.New(true)
	childB.Records = []Record{
		{Key: "h"},
		{Key: "i"},
		{Key: "j"},
	}

	root.Children = append(root.Children, childA.ID())
	root.Children = append(root.Children, childB.ID())
	root.insert(Record{Key: "g"})

	index.RootID = root.ID()

	for i, key := range []string{
		"c",
		"i",
	} {
		r, err := index.Get(context.Background(), key)
		if err != nil {
			t.Errorf("%d: Could not find record with key %s", i, key)
		}

		if r.Key != key {
			t.Errorf("%d: Want=%s Got=%s", i, key, r.Key)
		}
	}

	for i, key := range []string{
		"0",
		"5",
		"1000",
	} {
		r, err := index.Get(context.Background(), key)
		if err == nil {
			t.Errorf("%d: Expected missing key, but got %s", i, r.Key)
		}
	}
}
func TestInsert(t *testing.T) {
	t.Run("Index is saved if root changes", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		u := util{t, repo}
		index := New(2, repo)

		root, err := index.New(true)
		if err != nil {
			t.Fatal(err)
		}

		index.RootID = root.ID()

		index.insert(context.Background(), Record{Key: "k"})
		index.insert(context.Background(), Record{Key: "o"})
		index.insert(context.Background(), Record{Key: "s"})

		index.insert(context.Background(), Record{Key: "q"})
		repo.Flush()

		u.with("Root", index.RootID, func(nu namedUtil) {
			nu.hasNChildren(2)
		})

		u.with("New root", index.RootID, func(nu namedUtil) {
			if index.RootID == root.ID() {
				t.Errorf("Want=%s Got=%s", root.ID(), index.RootID)
			}

			if !reporter.Writes[index.RootID] {
				t.Errorf("New root not written")
			}

			nu.withChild(0, func(nu namedUtil) {
				if !reporter.Writes[nu.node.ID()] {
					t.Errorf("Child 0 not written")
				}
			})

			nu.withChild(1, func(nu namedUtil) {
				if !reporter.Writes[nu.node.ID()] {
					t.Errorf("Child 1 not written")
				}
			})
		})

	})

	t.Run("Insertion into tree with full root and full target leaf", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		u := util{t, repo}
		index := New(2, repo)

		root, _ := index.New(false)
		root.Records = []Record{
			{Key: "k"},
			{Key: "o"},
			{Key: "s"},
		}

		childA, _ := index.New(true)
		childA.Records = []Record{
			{Key: "a"},
			{Key: "b"},
			{Key: "c"},
		}
		childB, _ := index.New(true)
		childB.Records = []Record{
			{Key: "l"},
			{Key: "m"},
		}
		childC, _ := index.New(true)
		childC.Records = []Record{
			{Key: "p"},
			{Key: "q"},
		}
		childD, _ := index.New(true)
		childD.Records = []Record{
			{Key: "x"},
			{Key: "y"},
		}

		root.Children = append(root.Children, childA.ID())
		root.Children = append(root.Children, childB.ID())
		root.Children = append(root.Children, childC.ID())
		root.Children = append(root.Children, childD.ID())

		index.RootID = root.ID()

		ctx := context.Background()

		index.insert(ctx, Record{Key: "d"})
		index.insert(ctx, Record{Key: "r"})
		index.insert(ctx, Record{Key: "z"})

		if index.RootID == root.ID() {
			t.Errorf("[TestTreeInsert]: Expected new root")
		}

		u.with("root", index.RootID, func(nu namedUtil) {
			nu.hasNChildren(2)
			nu.hasKeys("o")

			nu.withChild(0, func(nu namedUtil) {
				nu.is(root)
				nu.hasKeys("b", "k")
				nu.hasNChildren(3)

				nu.withChild(0, func(nu namedUtil) {
					nu.hasKeys("a")
				})
				nu.withChild(1, func(nu namedUtil) {
					nu.hasKeys("c", "d")
				})
				nu.withChild(2, func(nu namedUtil) {
					nu.hasKeys("l", "m")
				})
			})

			nu.withChild(1, func(nu namedUtil) {
				nu.hasKeys("s")
				nu.hasNChildren(2)

				nu.withChild(0, func(nu namedUtil) {
					nu.hasKeys("p", "q", "r")
				})
				nu.withChild(1, func(nu namedUtil) {
					nu.hasKeys("x", "y", "z")
				})
			})
		})

	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("Index is saved if root becomes empty", func(t *testing.T) {
		repo, reporter := newMockRepo(2)
		index := New(2, repo)

		child, _ := index.New(true)
		child.Records = append(child.Records, Record{Key: "2"})

		deleteMe, _ := index.New(true)
		deleteMe.Records = append(deleteMe.Records, Record{Key: "8"})

		oldRoot, _ := index.New(false)
		oldRoot.Records = append(oldRoot.Records, Record{Key: "5"})
		oldRoot.Children = []string{
			child.ID(),
			deleteMe.ID(),
		}

		index.RootID = oldRoot.ID()

		err := index.Delete(ctx, "5")
		if err != nil {
			t.Fatal(err)
		}
		repo.Flush()

		if oldRoot.ID() == index.RootID {
			t.Error("Expected new Root ID")
		}

		if len(reporter.Writes) != 1 {
			t.Errorf("Want=2 Got=%d", len(reporter.Writes))
		}

		if !reporter.Writes[index.RootID] {
			t.Errorf("New root not written")
		}

		if len(reporter.Deletes) != 2 {
			t.Errorf("Want=2 Got=%d", len(reporter.Deletes))
		}

		if !reporter.Deletes[oldRoot.Name] {
			t.Errorf("Old root not deleted")
		}
		if !reporter.Deletes[deleteMe.Name] {
			t.Errorf("Right child not deleted")
		}
	})

	t.Run("Delete missing key", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		index := New(2, repo)

		root, err := index.New(false)
		if err != nil {
			log.Fatal(err)
		}

		index.RootID = root.ID()

		err = index.Delete(ctx, "10")
		if err == nil {
			t.Errorf("Deleting a missing key should return an error. Got=<nil>")
		}
	})

	t.Run("Delete key from tree with a single key", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		index := New(2, repo)
		u := util{t, repo}

		root, err := index.New(true)
		if err != nil {
			log.Fatal(err)
		}

		root.insert(Record{Key: "5"})
		index.RootID = root.ID()

		index.Delete(ctx, "5")

		u.with("Root", index.RootID, func(nu namedUtil) {
			nu.hasNDocs(0)
			nu.hasNChildren(0)
		})
	})

	t.Run("Delete from leaf", func(t *testing.T) {
		repo, _ := repository.NewMockRepo()
		ki := New(2, repo)
		root, _ := ki.New(true)
		root.insert(Record{Key: "1"})
		root.insert(Record{Key: "2"})
		root.insert(Record{Key: "3"})

		ki.RootID = root.ID()

		ki.Delete(ctx, "2")

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
			repo, _ := newMockRepo(2)
			index := New(2, repo)
			ctx := context.Background()

			for i := 0; i < b.N; i++ {
				index.insert(ctx, Record{Key: fmt.Sprintf("k%d", i)})
			}
		})
	}
}
