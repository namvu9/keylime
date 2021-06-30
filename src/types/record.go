package types

import (
	"strings"
	"time"
)

type Record struct {
	Key     string
	Value   []byte // Deprecated
	Data    map[string]Data
	TS      time.Time
	Deleted bool
}

type compareFunc func(*Record, *Record) int

// TODO: TEST
func (r *Record) Set(name string, value interface{}) {
	r.Data[name] = Data{
		Type: GetDataType(value),
		Value: value,
	}
}

func (r *Record) Get(name string) (Data, bool) {
	val, ok := r.Data[name]
	return val, ok
}

func (r Record) CreatedAt() time.Time {
	return r.TS
}

func (r *Record) IsLessThan(other *Record) bool {
	return r.Compare(byKey, other) < 0
}

func (r *Record) Compare(by compareFunc, other *Record) int {
	return by(r, other)
}

func (r Record) IsEqualTo(other *Record) bool {
	return r.Key == other.Key
}

func (r Record) String() string {
	return r.Key
}

func (r *Record) SetFields(fields map[string]interface{}) {
	for name, value := range fields {
		r.Data[name] = Data{
			Type:  GetDataType(value),
			Value: value,
		}
	}
}

func byKey(this, that *Record) int {
	return strings.Compare(this.Key, that.Key)
}

// TODO: Deprecate
func New(key string, value []byte) Record {
	return Record{
		Key:   key,
		Value: value,
		TS:    time.Now(),
		Data:  make(map[string]Data),
	}
}

func NewRecord(key string, value []byte) *Record {
	return &Record{
		Key:   key,
		Value: value,
		TS:    time.Now(),
		Data:  make(map[string]Data),
	}
}
