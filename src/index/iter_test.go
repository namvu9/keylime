package index

import "testing"

func TestMaxPage(t *testing.T) {
	max := makePage(2, makeDocs("11"))
	for _, test := range []struct {
		name string
		p    *Node
	}{
		{"from root",
			makePage(2, makeDocs("7"),
				makePage(2, makeDocs("5")),
				makePage(2, makeDocs("9"),
					makePage(2, makeDocs("8")),
					max,
				),
			),
		},

		{"from leaf", max},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, _ := test.p.maxPage().Get()

			if n != max {
				t.Errorf("Got=%v, Want=%v", n.Name, max.Name)
			}
		})
	}
}

func TestMinPage(t *testing.T) {
	min := makePage(2, makeDocs("11"))
	for _, test := range []struct {
		name string
		p    *Node
	}{
		{"from root",
			makePage(2, makeDocs("7"),
				makePage(2, makeDocs("5"),
					min,
					makePage(2, makeDocs("8")),
				),
				makePage(2, makeDocs("9")),
			),
		},

		{"from leaf", min},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, _ := test.p.minPage().Get()

			if n != min {
				t.Errorf("Got=%v, Want=%v", n.Name, min.Name)
			}
		})
	}
}
