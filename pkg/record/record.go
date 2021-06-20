package record

import (
	"strings"
	"time"
)

type Record struct {
	key   string
	value []byte
	ts    time.Time
}

func (r Record) Key() string {
  return r.key
}

func (r Record) CreatedAt() time.Time {
  return r.ts
}

func (r Record) Value() []byte {
  return r.value
}

func (r Record) IsLessThan(other Record) bool {
	return strings.Compare(r.key, other.key) < 0
}

func (r Record) IsEqualTo(other *Record) bool {
	return r.key == other.key
}


func New(key string, value []byte) Record {
  return Record{
    key: key,
    value: value,
    ts: time.Now(),
  }
}
