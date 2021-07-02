package store

import (
	"fmt"
	"testing"

	record "github.com/namvu9/keylime/src/types"
)

type util struct {
	t *testing.T
}

func (u util) with(name string, node *Page, fn func(namedUtil)) {
	fn(namedUtil{u, fmt.Sprintf("[%s]: %s", u.t.Name(), name), node})
}

func (u util) hasNRecords(name string, n int, node *Page) {
	if len(node.records) != n {
		u.t.Errorf("len(%s.records), Got=%d; Want=%d", name, len(node.records), n)
	}
}

func (u util) hasNChildren(name string, n int, node *Page) {
	if len(node.children) != n {
		u.t.Errorf("len(%s.children), Got=%d; Want=%d", name, len(node.children), n)
	}
}

func (u util) hasKeys(name string, keys []string, node *Page) {
	var nKeys []string
	for _, k := range node.records {
		nKeys = append(nKeys, k.Key)
	}
	errMsg := fmt.Sprintf("%s.records.keys, Got=%v; Want=%v", name, nKeys, keys)

	if len(node.records) != len(keys) {
		u.t.Errorf(errMsg)
		return
	}

	for i, r := range node.records {
		if r.Key != keys[i] {
			u.t.Errorf(errMsg)
		}
	}
}

func (u util) hasChildren(name string, children []*Page, node *Page) {
	wantIDs := []string{}
	for _, child := range children {
		wantIDs = append(wantIDs, fmt.Sprintf("%p", child))
	}

	gotIDs := []string{}
	for _, child := range node.children {
		gotIDs = append(gotIDs, fmt.Sprintf("%p", child))
	}

	errorMsg := fmt.Sprintf("%s.children, Got=%v; Want=%v", name, gotIDs, wantIDs)
	if len(node.children) != len(children) {
		u.t.Errorf(errorMsg)
	} else {
		for i, child := range children {
			if child != node.children[i] {
				u.t.Errorf(errorMsg)
				break
			}
		}
	}
}

type namedUtil struct {
	u    util
	name string
	node *Page
}

func (nu namedUtil) withChild(i int, fn func(namedUtil)) {
	child := nu.node.children[i]
	u := namedUtil{nu.u, fmt.Sprintf("[%s, child %d]", nu.name, i), child}
	fn(u)
}

func (nu namedUtil) is(other *Page) bool {
	return nu.node == other
}

func (nu namedUtil) hasNRecords(n int) {
	nu.u.hasNRecords(nu.name, n, nu.node)
}

func (nu namedUtil) hasNChildren(n int) {
	nu.u.hasNChildren(nu.name, n, nu.node)
}

func (nu namedUtil) hasKeys(keys ...string) {
	nu.u.hasKeys(nu.name, keys, nu.node)
}

func (nu namedUtil) hasChildren(children ...*Page) {
	nu.u.hasChildren(nu.name, children, nu.node)
}

func newPageWithKeys(t int, keys []string) *Page {
	return &Page{
		t:       t,
		records: makeRecords(keys...),
		loaded:  true,
	}
}

func makePageWithBufferedStorage(bs *WriteBuffer) func(t int, records []record.Record, children ...*Page) *Page {
	return func(t int, records []record.Record, children ...*Page) *Page {
		root := newPage(t, false, nil)
		root.writer = bs
		root.records = records
		root.children = children

		for _, child := range children {
			child.t = t
			child.loaded = true
		}

		if len(children) == 0 {
			root.leaf = true
		}

		return root

	}
}

func makePage(t int, records []record.Record, children ...*Page) *Page {
	root := newPage(t, false, nil)
	root.records = records
	root.children = children

	for _, child := range children {
		child.t = t
		child.loaded = true
	}

	if len(children) == 0 {
		root.leaf = true
	}

	return root
}

func makeRecords(keys ...string) []record.Record {
	out := []record.Record{}
	for _, key := range keys {
		out = append(out, record.New(key, nil))
	}

	return out
}

type Info struct {
	records []record.Record
	pages   []*Page
}

// Iterates over a collection in order of key precedence
func (info *Info) validate(p *Page, root bool) {
	if !root && len(p.records) < p.t-1 || len(p.records) > 2*p.t-1 {
		panic(fmt.Sprintf("Constraint violation: %s len_records = %d\n", p.ID, len(p.records)))
	}

	if !p.leaf {
		if len(p.children) != len(p.records)+1 {
			fmt.Printf("%s: Constraint violation: number of records should be len(children) - (%d) 1, but got %d\n", p.ID, len(p.children)-1, len(p.records))
		}
		for i, child := range p.children {
			if !child.loaded {
				child.load()
			}
			info.validate(child, false)
			if i < len(p.records) {
				r := p.records[i]
				info.records = append(info.records, r)
			}
		}
	} else {
		for _, r := range p.records {
			info.records = append(info.records, r)
		}
	}

	info.pages = append(info.pages, p)
}
