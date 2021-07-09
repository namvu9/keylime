package repository

import (
	"path"

	"github.com/namvu9/keylime/src/types"
)

func WithFactory(r Repository, f Factory) Repository {
	r.factory = f
	return r
}

// WithScope returns a repository scoped to `name` within
// the parent scope
func WithScope(r Repository, name string) Repository {
	r.scope = path.Join(r.scope, name)

	if _, ok := r.items[r.scope]; !ok {
		r.items[r.scope] = make(map[string]types.Identifier)
		r.buffer[r.scope] = make(map[string]types.Identifier)
	}

	return r
}
