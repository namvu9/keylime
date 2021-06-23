package store

import "testing"

func TestMaxPage(t *testing.T) {
	max := makePage(2, makeRecords("11"))
	for _, test := range []struct {
		name string
		p *Page
	}{
		{"from root",
			makePage(2, makeRecords("7"),
				makePage(2, makeRecords("5")),
				makePage(2, makeRecords("9"),
					makePage(2, makeRecords("8")),
					max,
				),
			),
		},

		{"from leaf", max},
	} {
		t.Run(test.name, func(t *testing.T) {
			n := test.p.Max().Get()

			if n != max {
				t.Errorf("Got=%v, Want=%v", n.ID, max.ID)
			}
		})
	}
}

func TestMinPage(t *testing.T) {
	min := makePage(2, makeRecords("11"))
	for _, test := range []struct {
		name string
		p *Page
	}{
		{"from root",
			makePage(2, makeRecords("7"),
				makePage(2, makeRecords("5"),
					min,
					makePage(2, makeRecords("8")),
				),
				makePage(2, makeRecords("9")),
			),
		},

		{"from leaf", min},
	} {
		t.Run(test.name, func(t *testing.T) {
			n := test.p.Min().Get()

			if n != min {
				t.Errorf("Got=%v, Want=%v", n.ID, min.ID)
			}
		})
	}
}
