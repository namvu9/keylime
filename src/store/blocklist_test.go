package store

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

func TestMain(m *testing.M) {
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestInsertBlockList(t *testing.T) {
	ctx := context.Background()

	t.Run("Normal insert", func(t *testing.T) {
		repo, reporter := repository.NewMockRepo()
		oi := newBlocklist(2, repo)
		doc := types.NewDoc("k")

		oi.create()
		oi.insert(ctx, doc)
		oi.repo.Flush()

		headNode, _ := oi.GetBlock(oi.Head)

		if got := len(headNode.Docs); got != 1 {
			t.Errorf("n records, want %d got %d", 1, got)
		}

		if headNode.Identifier != oi.Head {
			t.Errorf("Want %s Got %s", headNode.Identifier, oi.Head)
		}

		if _, ok := reporter.Writes[string(headNode.Identifier)]; !ok {
			t.Errorf("Head node was not written")
		}

		if first := headNode.Docs[0]; first.Key != doc.Key || first.Key != "k" {
			t.Errorf("Expected first record to have key k, got %s", first.Key)
		}
	})

	t.Run("Insertion into full node", func(t *testing.T) {
		repo, reporter := repository.NewMockRepo()
		oi := newBlocklist(2, repo)
		oi.create()
		oldHead, _ := oi.GetBlock(oi.Head)
		oldHead.Docs = []types.Document{types.NewDoc("nil"), types.NewDoc("HAHA")}

		r := types.NewDoc("o")
		oi.insert(ctx, r)
		oi.repo.Flush()

		if oi.Head == oi.Tail {
			t.Errorf("Expected new node to be allocated")
		}

		if oi.Tail != oldHead.Identifier {
			t.Errorf("Expected old head to be new tail")
		}

		newHead, _ := oi.GetBlock(oi.Head)
		if newHead.Next != oi.Tail {
			t.Errorf("New head does not reference old head")
		}

		if oldHead.Prev != oi.Head {
			t.Errorf("Old head does not reference new head")
		}

		if _, ok := reporter.Writes[string(oldHead.Identifier)]; !ok {
			t.Errorf("Old head was not written")
		}

		if _, ok := reporter.Writes[string(oi.Head)]; !ok {
			t.Errorf("New head was not written")
		}
	})
}

func TestDeleteBlockList(t *testing.T) {
	repo, reporter := repository.NewMockRepo()
	oi := newBlocklist(2, repo)
	doc := types.NewDoc("k")
	oi.create()

	headNode, err := oi.GetBlock(oi.Head)
	oi.insert(context.Background(), doc)

	if headNode.Docs[0].Deleted {
		t.Errorf("Newly inserted documents should not be deleted")
	}

	oi.remove(context.Background(), doc.Key)

	if err != nil {
		t.Error(err)
	}

	if !headNode.Docs[0].Deleted {
		t.Errorf("Expected document with key k to be deleted")
	}

	oi.repo.Flush()
	if _, ok := reporter.Writes[headNode.ID()]; !ok {
		t.Errorf("Node was not written")
	}
}

func TestUpdateBlockList(t *testing.T) {
	repo, reporter := repository.NewMockRepo()
	oi := newBlocklist(2, repo)
	oi.create()
	doc := types.NewDoc("k")

	headNode, err := oi.GetBlock(oi.Head)
	if err != nil {
		t.Error(err)
	}
	oi.insert(context.Background(), doc)

	doc.Set(map[string]interface{}{
		"LOL": 4,
	})

	oi.update(context.Background(), doc)

	oi.repo.Flush()
	if _, ok := reporter.Writes[headNode.ID()]; !ok {
		t.Errorf("Node was not written")
	}

}

func newMockRepo(blockSize int) (repository.Repository, *repository.IOReporter) {
	repo, reporter := repository.NewMockRepo()

	return repository.WithFactory(repo, &BlockFactory{capacity: blockSize, repo: repo}), reporter
}

func TestGetBlockList(t *testing.T) {
	ctx := context.Background()

	t.Run("Desc: n < records in index", func(t *testing.T) {
		repo, _ := newMockRepo(2)
		bl := newBlocklist(2, repo)

		bl.create()

		d := types.NewDoc("d")
		d.Deleted = true

		bl.insert(ctx, types.NewDoc("a"))
		bl.insert(ctx, types.NewDoc("b"))
		bl.insert(ctx, types.NewDoc("c"))
		bl.insert(ctx, d)
		bl.insert(ctx, types.NewDoc("e"))

		res := bl.GetN(4, false)

		if len(res) != 4 {
			t.Fatalf("Want %d Got %d", 4, len(res))
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
		repo, _ := repository.NewMockRepo()
		oi := newBlocklist(2, repo)
		oi.create()

		d := types.NewDoc("d")
		d.Deleted = true

		oi.insert(ctx, types.NewDoc("a"))
		oi.insert(ctx, types.NewDoc("b"))
		oi.insert(ctx, types.NewDoc("c"))
		oi.insert(ctx, d)
		oi.insert(ctx, types.NewDoc("e"))

		res := oi.GetN(100, false)

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
		repo, _ := newMockRepo(2)
		oi := newBlocklist(2, repo)
		oi.create()

		d := types.NewDoc("d")
		d.Deleted = true

		oi.insert(ctx, types.NewDoc("a"))
		oi.insert(ctx, types.NewDoc("b"))
		oi.insert(ctx, types.NewDoc("c"))
		oi.insert(ctx, d)
		oi.insert(ctx, types.NewDoc("e"))

		res := oi.GetN(4, true)

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
		repo, _ := newMockRepo(2)
		oi := newBlocklist(2, repo)
		oi.create()

		d := types.NewDoc("d")
		d.Deleted = true

		oi.insert(ctx, types.NewDoc("a"))
		oi.insert(ctx, types.NewDoc("b"))
		oi.insert(ctx, types.NewDoc("c"))
		oi.insert(ctx, d)
		oi.insert(ctx, types.NewDoc("e"))

		res := oi.GetN(100, true)

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

func TestCreateBlocklist(t *testing.T) {
	t.Run("Block list", func(t *testing.T) {
		repo, reporter := repository.NewMockRepo()
		oi := newBlocklist(10, repo)

		err := oi.create()
		if err != nil {
			t.Errorf("Unexpected error %s", err)
		}

		oi.repo.Flush()

		if oi.Head != oi.Tail {
			t.Errorf("Expected head (%s) and tail (%s) to be equal", oi.Head, oi.Tail)
		}

		if _, ok := reporter.Writes[string(oi.Head)]; !ok {
			t.Errorf("Did not write Head/Tail node")
		}
	})
}
