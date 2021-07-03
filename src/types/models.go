package types

import "context"

type Store interface {
	Collection(name string) Collection
}

type Collection interface {
	Get(ctx context.Context, k string) (*Record, error)
	GetFirst(ctx context.Context, n int) ([]*Record, error)
	GetLast(ctx context.Context, n int) ([]*Record, error)

	Set(ctx context.Context, k string, fields map[string]interface{}) error
	Delete(ctx context.Context, k string) error
	Update(ctx context.Context, k string, fields map[string]interface{}) error
	Create(ctx context.Context, s *Schema) error

	Info(ctx context.Context)
}
