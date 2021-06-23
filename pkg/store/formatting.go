package store

import (
	"fmt"
	"strings"
)

func (b *Page) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "-----\nBNode\n-----\n")
	if b.ID != "" {
		fmt.Fprintf(&sb, "ID:\t\t%s\n", b.ID)
	} else {
		fmt.Fprint(&sb, "ID:\t\t<NONE>\n")
	}
	fmt.Fprintf(&sb, "t:\t\t%d\n", b.t)
	fmt.Fprintf(&sb, "Loaded:\t\t%v\n", b.loaded)
	fmt.Fprintf(&sb, "Leaf:\t\t%v\n", b.leaf)
	fmt.Fprintf(&sb, "Children:\t%v\n", len(b.children))
	fmt.Fprintf(&sb, "Keys:\t\t")
	for _, key := range b.records {
		fmt.Fprintf(&sb, "%v ", key)
	}
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
