package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func Prettify(v interface{}) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type Record struct {
	Key     string
	Value   []byte // Deprecated
	Data    map[string]Data
	TS      time.Time
	Deleted bool
}

type compareFunc func(Record, Record) int

// TODO: TEST
func (r *Record) Set(name string, value interface{}) {
	r.Data[name] = Data{
		Type:  GetDataType(value),
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

func (r *Record) IsLessThan(other Record) bool {
	return r.Compare(byKey, other) < 0
}

func (r Record) Compare(by compareFunc, other Record) int {
	return by(r, other)
}

func (r Record) IsEqualTo(other *Record) bool {
	return r.Key == other.Key
}

func (r Record) String() string {
	s, _ := Prettify(r.Data)
	return fmt.Sprintf("%s=%s", r.Key, s)
}

func (r *Record) SetFields(fields map[string]interface{}) {
	r.Data = make(map[string]Data)

	for name, value := range fields {
		r.Data[name] = Data{
			Type:  GetDataType(value),
			Value: value,
		}
	}
}

func (r *Record) Clone() *Record {
	clone := NewRecord(r.Key)
	for name, data := range r.Data {
		clone.Data[name] = data
	}

	return clone
}

// TODO: Make sure original isn't affected
func (r *Record) UpdateFields(fields map[string]interface{}) *Record {
	c := r.Clone()
	
	for name, value := range fields {
		c.Data[name] = Data{
			Type:  GetDataType(value),
			Value: value,
		}
	}
	

	return c
}

func byKey(this, that Record) int {
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

func NewRecord(key string) *Record {
	return &Record{
		Key:  key,
		TS:   time.Now(),
		Data: make(map[string]Data),
	}
}
