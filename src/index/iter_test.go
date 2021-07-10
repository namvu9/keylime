package index

import (
	"context"
	"testing"
)

func TestMaxNode(t *testing.T) {
	for _, test := range []struct {
		name string
		keys []string
	}{
		{"from root", []string{"7", "5", "8", "9"}},
		{"from leaf", []string{"11"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, _ := newMockRepo(2)
			index := New(2, repo)
			u := util{t, repo}

			root, _ := index.New(true)
			index.RootID = root.ID()

			for _, key := range test.keys {
				index.insert(context.Background(), Record{Key: key})
			}

			u.with("root", index.RootID, func(nu namedUtil) {
				n, err := nu.node.maxNode().Get()
				if err != nil {
					t.Fatal(err)
				}

				_, ok := n.keyIndex(test.keys[len(test.keys)-1])
				if !ok {
					t.Errorf("maxNode did not return node with largest key")
				}
			})
		})
	}
}

func TestMinNode(t *testing.T) {
	for _, test := range []struct {
		name string
		keys []string
	}{
		{"from root", []string{"5", "7", "8", "9"}},
		{"from leaf", []string{"11"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, _ := newMockRepo(2)
			index := New(2, repo)
			u := util{t, repo}

			root, _ := index.New(true)
			index.RootID = root.ID()

			for _, key := range test.keys {
				index.insert(context.Background(), Record{Key: key})
			}

			u.with("root", index.RootID, func(nu namedUtil) {
				n, err := nu.node.minNode().Get()
				if err != nil {
					t.Fatal(err)
				}

				_, ok := n.keyIndex(test.keys[0])
				if !ok {
					t.Errorf("minNode did not return node with smallest key")
				}
			})
		})
	}
}
