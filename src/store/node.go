package store

type PageFactory struct{}

func (pf PageFactory) Load(id string) (interface{}, error) { return nil, nil }
func (pf PageFactory) New() (interface{}, error)           { return nil, nil }
func (pf PageFactory) Save(p *Page) error                  { return nil }
