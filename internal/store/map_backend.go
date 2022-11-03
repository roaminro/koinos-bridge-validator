package store

import (
	"encoding/hex"
	"errors"
)

// MapBackend implements a key-value store backed by a simple map
type MapBackend struct {
	storage map[string][]byte
}

// NewMapBackend creates and returns a reference to a map backend instance
func NewMapBackend() *MapBackend {
	return &MapBackend{make(map[string][]byte)}
}

// Reset resets the database
func (backend *MapBackend) Reset() error {
	backend.storage = make(map[string][]byte)
	return nil
}

// Put adds the requested value to the database
func (backend *MapBackend) Put(key []byte, value []byte) error {
	if key == nil {
		return errors.New("cannot put a nil value key")
	}
	if value == nil {
		return errors.New("cannot put a nil value")
	}
	k := hex.EncodeToString(key)
	//fmt.Println("Putting key:", k)
	backend.storage[k] = value
	return nil
}

// Get fetches the requested value from the database
func (backend *MapBackend) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key cannot be empty")
	}
	k := hex.EncodeToString(key)
	//fmt.Println("Getting key:", k)
	val, ok := backend.storage[k]
	if ok {
		return val, nil
	}

	return make([]byte, 0), nil
}
