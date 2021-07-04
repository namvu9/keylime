package store

import (
	"fmt"
	"strings"
)

func (p Page) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "-----\nPage\n-----\n")
	if p.ID != "" {
		fmt.Fprintf(&sb, "ID:\t\t%s\n", p.ID)
	} else {
		fmt.Fprint(&sb, "ID:\t\t<NONE>\n")
	}
	fmt.Fprintf(&sb, "t:\t\t%d\n", p.t)
fmt.Fprintf(&sb, "Loaded:\t\t%v\n", p.loaded)
fmt.Fprintf(&sb, "Leaf:\t\t%v\n", p.leaf)
fmt.Fprintf(&sb, "Children:\t%v\n", len(p.children))
fmt.Fprintf(&sb, "Docs:\t")
for _, r := range p.docs {
	fmt.Fprintf(&sb, "%v ", r)
}
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}

func (ki KeyIndex) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n-----\nKeyIndex\n-----\n")
	fmt.Fprintf(&sb, "Height:\t%d\n", ki.Height)
	fmt.Fprintf(&sb, "\n")
	return sb.String()
}
