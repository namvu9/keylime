package store

import (
	"bytes"
	"encoding/gob"
)

func (b *page) GobEncode() ([]byte, error) {
	refs := []*page{}
	for _, c := range b.children {
		cNode := new(page)
		cNode.ID = c.ID
		cNode.leaf = c.leaf
		refs = append(refs, cNode)
	}
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)

	err := encoder.Encode(refs)
	if err != nil {
		return nil, err
	}
	encoder.Encode(b.ID)
	encoder.Encode(b.t)
	encoder.Encode(b.leaf)
	encoder.Encode(b.records)

	return w.Bytes(), nil
}

func (b *page) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)

	if err := decoder.Decode(&b.children); err != nil {
		return err
	}

	if err := decoder.Decode(&b.ID); err != nil {
		return err
	}
	if err := decoder.Decode(&b.t); err != nil {
		return err
	}
	if err := decoder.Decode(&b.leaf); err != nil {
		return err
	}
	if err := decoder.Decode(&b.records); err != nil {
		return err
	}

	return nil
}

