package store

import (
	"fmt"
	"sync"

	"github.com/koinos-bridge/koinos-bridge-validator/proto/build/github.com/koinos-bridge/koinos-bridge-validator/bridge_pb"
	"google.golang.org/protobuf/proto"
)

// TransactionsStore contains a backend object and handles requests
type TransactionsStore struct {
	backend Backend
	rwmutex sync.RWMutex
	sync.Mutex
}

// NewTransactionsStore creates a new TransactionsStore wrapping the provided backend
func NewTransactionsStore(backend Backend) *TransactionsStore {
	return &TransactionsStore{backend: backend}
}

func (handler *TransactionsStore) Put(key string, transaction *bridge_pb.Transaction) error {
	handler.rwmutex.Lock()
	defer handler.rwmutex.Unlock()

	itemBytes, err := proto.Marshal(transaction)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrSerialization, err)
	}

	err = handler.backend.Put([]byte(key), itemBytes)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrBackend, err)
	}

	return nil
}

func (handler *TransactionsStore) Get(key string) (*bridge_pb.Transaction, error) {
	handler.rwmutex.RLock()
	defer handler.rwmutex.RUnlock()

	itemBytes, err := handler.backend.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("%w, %v", ErrBackend, err)
	}

	if len(itemBytes) != 0 {
		item := &bridge_pb.Transaction{}
		if err := proto.Unmarshal(itemBytes, item); err != nil {
			return nil, fmt.Errorf("%w, %v", ErrDeserialization, err)
		}

		return item, nil
	}

	return nil, nil
}
