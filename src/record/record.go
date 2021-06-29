package record

import (
	"strings"
	"time"
)

type Record struct {
	Key     string
	Value   []byte
	TS      time.Time
	Deleted bool
}

func (r Record) CreatedAt() time.Time {
	return r.TS
}

func (r Record) IsLessThan(other Record) bool {
	return strings.Compare(r.Key, other.Key) < 0
}

func (r Record) IsEqualTo(other *Record) bool {
	return r.Key == other.Key
}

func (r Record) String() string {
	return r.Key
}

func New(key string, value []byte) Record {
	return Record{
		Key:   key,
		Value: value,
		TS:    time.Now(),
	}
}
