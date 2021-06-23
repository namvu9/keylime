package store

type ChangeReporter struct {
	writes  []*Page
	deletes []*Page
}

func (cr *ChangeReporter) Write(b *Page, reason string) {
	for _, write := range cr.writes {
		if write.ID == b.ID {
			return
		}
	}
	cr.writes = append(cr.writes, b)
}

func (cr *ChangeReporter) Delete(b *Page, reason string) {
	for _, del := range cr.writes {
		if del.ID == b.ID {
			return
		}
	}
	cr.deletes = append(cr.deletes, b)
}

