package store

type ChangeReporter struct {
	writes  []*BNode
	deletes []*BNode
}

func (cr *ChangeReporter) Write(b *BNode, reason string) {
	for _, write := range cr.writes {
		if write.ID == b.ID {
			return
		}
	}
	cr.writes = append(cr.writes, b)
}

func (cr *ChangeReporter) Delete(b *BNode, reason string) {
	for _, del := range cr.writes {
		if del.ID == b.ID {
			return
		}
	}
	cr.deletes = append(cr.deletes, b)
}

