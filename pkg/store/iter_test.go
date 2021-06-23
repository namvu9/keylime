package store

import "testing"

func TestMaxPage(t *testing.T) {
	max := makeTree(2, makeRecords("11"))
	for _, test := range []struct {
		name string
		p *Page
	}{
		{"from root",
			makeTree(2, makeRecords("7"),
				makeTree(2, makeRecords("5")),
				makeTree(2, makeRecords("9"),
					makeTree(2, makeRecords("8")),
					max,
				),
			),
		},

		{"from leaf", max},
	} {
		t.Run(test.name, func(t *testing.T) {
			n := test.p.MaxPage()

			if n != max {
				t.Errorf("Got=%v, Want=%v", n.ID, max.ID)
			}
		})
	}
}

func TestMinPage(t *testing.T) {
	min := makeTree(2, makeRecords("11"))
	for _, test := range []struct {
		name string
		p *Page
	}{
		{"from root",
			makeTree(2, makeRecords("7"),
				makeTree(2, makeRecords("5"),
					min,
					makeTree(2, makeRecords("8")),
				),
				makeTree(2, makeRecords("9")),
			),
		},

		{"from leaf", min},
	} {
		t.Run(test.name, func(t *testing.T) {
			n := test.p.MinPage()

			if n != min {
				t.Errorf("Got=%v, Want=%v", n.ID, min.ID)
			}
		})
	}
}
