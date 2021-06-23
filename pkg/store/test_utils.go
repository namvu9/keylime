package store

import (
	"fmt"
	"testing"

	"github.com/namvu9/keylime/pkg/record"
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
	errMsg := fmt.Sprintf("%s.records.keys, Got=%v; Want=%v", name, node.records, keys)

	if len(node.records) != len(keys) {
		u.t.Errorf(errMsg)
	}

	for i, r := range node.records {
		if r.Key() != keys[i] {
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

func makeNewRecords(keys []string) (out []record.Record) {
	for _, k := range keys {
		out = append(out, record.New(k, nil))
	}

	return
}

func newNodeWithKeys(t int, keys []string) *Page {
	return &Page{
		t:       t,
		records: makeNewRecords(keys),
	}
}

func makePage(t int, records []record.Record, children ...*Page) *Page {
	root := newPage(t)
	root.records = records
	root.children = children

	for _, child := range children {
		child.t = t
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

