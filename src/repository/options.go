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

	r.items[r.scope] = make(map[string]types.Identifiable)
	r.buffer[r.scope] = make(map[string]types.Identifiable)
	return r
}