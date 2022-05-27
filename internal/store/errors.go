package store

import "errors"

var (
	// ErrSerialization occurs when a type cannot be serialized correctly
	ErrSerialization = errors.New("error serializing type")

	// ErrDeserialization occurs when a type cannot be deserialized correctly
	ErrDeserialization = errors.New("error deserializing type")

	// ErrBackend occurs when there is an error in the backend
	ErrBackend = errors.New("error in backend")
)
