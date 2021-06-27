package store

// Config represnts the configuration used to initialize the
// store
type Config struct {
	Name    string `json:"name"`
	BaseDir string `json:"baseDir"`
	T       int    `json:"minDegree"` // Only applies when creating new stores
	Storage ReadWriterTo
}
