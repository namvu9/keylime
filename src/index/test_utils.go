package index

import (
	"fmt"
	"testing"

	"github.com/namvu9/keylime/src/repository"
	record "github.com/namvu9/keylime/src/types"
)

type util struct {
	t    *testing.T
	repo repository.Repository
}

func (u util) with(name string, id string, fn func(namedUtil)) {
	item, _ := u.repo.Get(id)
	page, _ := item.(*Node)

	fn(namedUtil{u, fmt.Sprintf("[%s]: %s", u.t.Name(), name), page})
}

func (u util) hasNDocs(name string, n int, node *Node) {
	if len(node.Docs) != n {
		u.t.Errorf("len(%s.records), Got=%d; Want=%d", name, len(node.Docs), n)
	}
}

func (u util) hasNChildren(name string, n int, node *Node) {
	if len(node.Children) != n {
		u.t.Errorf("len(%s.children), Got=%d; Want=%d", name, len(node.Children), n)
	}
}

func (u util) hasKeys(name string, keys []string, node *Node) {
	var nKeys []string
	for _, k := range node.Docs {
		nKeys = append(nKeys, k.Key)
	}
	errMsg := fmt.Sprintf("%s.records.keys, Got=%v; Want=%v", name, nKeys, keys)

	if len(node.Docs) != len(keys) {
		u.t.Errorf(errMsg)
		return
	}

	for i, r := range node.Docs {
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

func newPageWithKeys(t int, keys []string) *Node {
	return &Node{
		T: t,
	}
}

func makePageWithBufferedStorage(bs interface{}) func(t int, records []record.Document, children ...*Node) *Node {
	return func(t int, records []record.Document, children ...*Node) *Node {
		//root := newPage(t, false)
		////root.docs = records
		//root.children = children

		//for _, child := range children {
		//child.t = t
		//child.loaded = true
		//}

		//if len(children) == 0 {
		//root.leaf = true
		//}

		//return root

		return nil
	}
}

func makePage(t int, records []record.Document, children ...*Node) *Node {
	//root := newPage(t, false)
	//root.docs = records
	//root.children = children

	//for _, child := range children {
	//child.t = t
	//child.loaded = true
	//}

	//if len(children) == 0 {
	//root.leaf = true
	//}

	//return root
	return nil
}

func makeDocs(keys ...string) []record.Document {
	out := []record.Document{}
	for _, key := range keys {
		out = append(out, record.NewDoc(key))
	}

	return out
}

type Info struct {
	docs []Record
	pages   []*Node
}

// Iterates over a collection in order of key precedence
func (info *Info) validate(p *Node, root bool) {
	if !root && len(p.Docs) < p.T-1 || len(p.Docs) > 2*p.T-1 {
		panic(fmt.Sprintf("Constraint violation: %s len_records = %d\n", p.Name, len(p.Docs)))
	}

	if !p.Leaf {
		if len(p.Children) != len(p.Docs)+1 {
			fmt.Printf("%s: Constraint violation: number of records should be len(children) - (%d) 1, but got %d\n", p.Name, len(p.Children)-1, len(p.Docs))
		}
		for i := range p.Children {
			child, _ := p.child(i)
			info.validate(child, false)
			if i < len(p.Docs) {
				r := p.Docs[i]
				info.docs = append(info.docs, r)
			}
		}
	} else {
		for _, r := range p.Docs {
			info.docs = append(info.docs, r)
		}
	}

	info.pages = append(info.pages, p)
}
