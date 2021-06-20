package store

import "strings"

type RecordKey string

type Record struct {
	Key   string
	Value []byte
}

type Records []Record

func (r Records) equals(other Records) bool {
	if len(r) != len(other) {
		return false
	}

	for i, k := range r {
		if k.Key != other[i].Key {
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
		if r.Key != keys[i] {
			return false
		}
	}

	return true
}

func (r Records) keys() (out []string) {
	for _, r := range r {
		out = append(out, r.Key)
	}
	return
}

func (r Records) values() (out [][]byte) {
	for _, record := range r {
		out = append(out, record.Value)
	}
	return
}

func (r Records) last() Record {
	return r[len(r)-1]
}

func (r Record) isLessThan(other Record) bool {
	return strings.Compare(r.Key, other.Key) < 0
}

func (r Record) isEqualTo(other *Record) bool {
	return r.Key == other.Key
}

func NewRecord(key string, value []byte) Record {
	return Record{key, value}
}
