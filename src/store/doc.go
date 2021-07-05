/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import "encoding/gob"

func init() {
	gob.Register(&OrderIndex{})
	gob.Register(&Node{})
}
