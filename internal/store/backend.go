package store

// Backend interface defines an abstract key-value store
type Backend interface {
	/**
	 * Store the given value in the given key.
	 */
	Put(key []byte, value []byte) error

	/**
	 * Get a previously stored value.
	 *
	 * If the key is not found, returns (nil, nil).
	 */
	Get(key []byte) ([]byte, error)

	// Resets the entire database
	Reset() error
}
