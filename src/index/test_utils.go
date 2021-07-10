package index

import (
	"fmt"
	"testing"

	"github.com/namvu9/keylime/src/repository"
)

type NodeMaker func([]Record, ...*Node) *Node

func TreeFactory(repo repository.Repository) NodeMaker {
	return func(r []Record, children ...*Node) *Node {
		n, _ := repo.New().(*Node)
		n.Records = r

		for _, child := range children {
			n.Children = append(n.Children, child.ID())
		}

		if len(children) == 0 {
			n.Leaf = true
		}

		return n
	}
}

func MakeRecords(keys ...string) []Record {
	var out []Record
	for _, key := range keys {
		out = append(out, Record{Key: key})
	}

	return out
}


type util struct {
	t    *testing.T
	repo repository.Repository
}

func (u util) with(name string, id string, fn func(namedUtil)) {
	item, err := u.repo.Get(id)
	if err != nil {
		u.t.Fatal(err)
	}

	page, ok := item.(*Node)
	if !ok {
		fmt.Println("><>>", id, name)
		u.t.Fatal(ok)
	}

	fn(namedUtil{u, fmt.Sprintf("[%s]: %s", u.t.Name(), name), page})
}

func (u util) hasNDocs(name string, n int, node *Node) {
	if len(node.Records) != n {
		u.t.Errorf("len(%s.records), Got=%d; Want=%d", name, len(node.Records), n)
	}
}

func (u util) hasNChildren(name string, n int, node *Node) {
	if len(node.Children) != n {
		u.t.Errorf("len(%s.children), Got=%d; Want=%d", name, len(node.Children), n)
	}
}

func (u util) hasKeys(name string, keys []string, node *Node) {
	var nKeys []string
	for _, k := range node.Records {
		nKeys = append(nKeys, k.Key)
	}
	errMsg := fmt.Sprintf("%s.records.keys, Got=%v; Want=%v", name, nKeys, keys)

	if len(node.Records) != len(keys) {
		u.t.Errorf(errMsg)
		return
	}

	for i, r := range node.Records {
		if r.Key != keys[i] {
			u.t.Errorf(errMsg)
		}
	}
}

func (u util) hasChildren(name string, children []*Node, node *Node) {
	wantIDs := []string{}
	for _, child := range children {
		wantIDs = append(wantIDs, fmt.Sprintf("%p", child))
	}

	gotIDs := []string{}
	for _, child := range node.Children {
		gotIDs = append(gotIDs, fmt.Sprintf("%s", child))
	}

	errorMsg := fmt.Sprintf("%s.children, Got=%v; Want=%v", name, gotIDs, wantIDs)
	if len(node.Children) != len(children) {
		u.t.Errorf(errorMsg)
	} else {
		for i, child := range children {
			if child.ID() != node.Children[i] {
				u.t.Errorf(errorMsg)
				break
			}
		}
	}
}

type namedUtil struct {
	u    util
	name string
	node *Node
}

func (nu namedUtil) withChild(i int, fn func(namedUtil)) {
	child, _ := nu.node.child(i)

	u := namedUtil{nu.u, fmt.Sprintf("[%s, child %d]", nu.name, i), child}
	fn(u)
}

func (nu namedUtil) is(other *Node) bool {
	return nu.node == other
}

func (nu namedUtil) hasNDocs(n int) {
	nu.u.hasNDocs(nu.name, n, nu.node)
}

func (nu namedUtil) hasNChildren(n int) {
	nu.u.hasNChildren(nu.name, n, nu.node)
}

func (nu namedUtil) hasKeys(keys ...string) {
	nu.u.hasKeys(nu.name, keys, nu.node)
}

func (nu namedUtil) hasChildren(children ...*Node) {
	nu.u.hasChildren(nu.name, children, nu.node)
}
