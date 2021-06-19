package store

import (
	"fmt"
	"testing"
)

func makeNewKeys(keys []string) (out []*Record) {
	for _, k := range keys {
		out = append(out, NewRecord(k, nil))
	}

	return
}

func newNodeWithKeys(t int, keys []string) *BNode {
	return &BNode{
		T:       t,
		Records: makeNewKeys(keys),
	}
}

func makeTree(t int, records []*Record, children ...*BNode) *BNode {
	root := newNode(t)
	root.Records = records
	root.children = children

	for _, child := range children {
		child.T = t
	}

	if len(children) == 0 {
		root.Leaf = true
	}

	return root
}

func makeRecords(keys ...string) []*Record {
	out := []*Record{}
	for _, key := range keys {
		out = append(out, &Record{key, nil})
	}

	return out
}

type util struct {
	t *testing.T
}

type namedUtil struct {
	u    util
	name string
	node *BNode
}

func (nu namedUtil) is(other *BNode) bool {
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

func (nu namedUtil) hasChildren(children ...*BNode) {
	nu.u.hasChildren(nu.name, children, nu.node)
}

func (u util) with(name string, node *BNode, fn func(namedUtil)) {
	fn(namedUtil{u, fmt.Sprintf("[%s]: %s", u.t.Name(), name), node})
}

func (u util) hasNRecords(name string, n int, node *BNode) {
	if len(node.Records) != n {
		u.t.Errorf("len(%s.records), Got=%d; Want=%d", name, len(node.Records), n)
	}
}

func (u util) hasNChildren(name string, n int, node *BNode) {
	if len(node.children) != n {
		u.t.Errorf("len(%s.children), Got=%d; Want=%d", name, len(node.children), n)
	}
}

func (u util) hasKeys(name string, keys []string, node *BNode) {
	if !node.Records.contains(keys) {
		u.t.Errorf("%s.records.keys, Got=%v; Want=%v", name, node.Records.keys(), keys)
	}
}

func (u util) hasChildren(name string, children []*BNode, node *BNode) {
	errorMsg := fmt.Sprintf("%s.children, Got=%v; Want=%v", name, node.children, children)
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
