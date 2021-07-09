package index

import "encoding/gob"

func init() {
	gob.Register(&Node{})
}
