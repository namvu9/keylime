/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import "encoding/gob"

func init() {
	gob.Register(&Blocklist{})
	gob.Register(&Block{})
	gob.Register(&Collection{})
}
