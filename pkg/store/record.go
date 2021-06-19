package store

import "strings"

type RecordKey string

type Record struct {
	key   string
	value []byte
}

type Records []*Record

func (r Records) equals(other Records) bool {
	if len(r) != len(other) {
		return false
	}

	for i, k := range r {
		if k.key != other[i].key {
			return false
		}
	}

	return true
}

func (r Records) contains(keys []string) bool {
	if len(r) != len(keys) {
		return false
	}

	for i, r := range r {
		if r.key != keys[i] {
			return false
		}
	}

	return true
}

func (r Records) keys() (out []string) {
	for _, r := range r {
		out = append(out, r.key)
	}
	return
}

func (r Records) values() (out [][]byte) {
	for _, record := range r {
		out = append(out, record.value)
	}
	return
}

func (r Records) last() *Record {
	return r[len(r)-1]
}

func (r Record) isLessThan(other *Record) bool {
	return strings.Compare(r.key, other.key) < 0
}

func (r Record) isEqualTo(other *Record) bool {
	return r.key == other.key
}

func NewRecord(key string, value []byte) *Record {
	return &Record{key, value}
}
