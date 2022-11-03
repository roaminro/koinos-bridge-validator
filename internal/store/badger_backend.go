package store

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

// BadgerBackend Badger backend implementation
type BadgerBackend struct {
	DB *badger.DB
}

// NewBadgerBackend BadgerBackend constructor
func NewBadgerBackend(opts badger.Options) *BadgerBackend {
	badgerDB, _ := badger.Open(opts)
	return &BadgerBackend{DB: badgerDB}
}

// Close cleans backend resources
func (backend *BadgerBackend) Close() {
	backend.DB.Close()
}

// Reset resets the database
func (backend *BadgerBackend) Reset() error {
	return backend.DB.DropAll()
}

// Put backend setter
func (backend *BadgerBackend) Put(key, value []byte) error {
	if value == nil {
		return errors.New("cannot put a nil value")
	}
	return backend.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get backend getter
func (backend *BadgerBackend) Get(key []byte) ([]byte, error) {
	var value []byte = nil
	err := backend.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			value = make([]byte, 0)
			return nil
		} else if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		return err
	})

	return value, err
}

// KoinosBadgerLogger implements the badger.Logger interface in roder to pass badger logs the the koinos logger
type KoinosBadgerLogger struct {
}

// Errorf implements formatted error message handling for badger
func (kbl KoinosBadgerLogger) Errorf(msg string, args ...interface{}) {
	zap.S().Errorf(strings.TrimSpace(msg), args...)
}

// Warningf implements formatted warning message handling for badger
func (kbl KoinosBadgerLogger) Warningf(msg string, args ...interface{}) {
	zap.S().Warnf(strings.TrimSpace(msg), args...)
}

// Infof implements formatted info message handling for badger
func (kbl KoinosBadgerLogger) Infof(msg string, args ...interface{}) {
	zap.S().Infof(strings.TrimSpace(msg), args...)
}

// Debugf implements formatted debug message handling for badger
func (kbl KoinosBadgerLogger) Debugf(msg string, args ...interface{}) {
	zap.S().Debugf(strings.TrimSpace(msg), args...)
}
