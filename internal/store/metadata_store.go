package store

import (
	"fmt"
	"sync"

	"github.com/roaminroe/koinos-bridge-validator/proto/build/github.com/roaminroe/koinos-bridge-validator/bridge_pb"
	"google.golang.org/protobuf/proto"
)

const (
	MetadaKey = "MetadaKey"
)

// MetadataStore contains a backend object and handles requests
type MetadataStore struct {
	backend Backend
	rwmutex sync.RWMutex
	sync.Mutex
}

// NewMetadataStore creates a new MetadataStore wrapping the provided backend
func NewMetadataStore(backend Backend) *MetadataStore {
	return &MetadataStore{backend: backend}
}

func (handler *MetadataStore) Put(metadata *bridge_pb.Metadata) error {
	handler.rwmutex.Lock()
	defer handler.rwmutex.Unlock()

	itemBytes, err := proto.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrSerialization, err)
	}

	err = handler.backend.Put([]byte(MetadaKey), itemBytes)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrBackend, err)
	}

	return nil
}

// GetContractMeta returns transactions by transaction ID
func (handler *MetadataStore) Get() (*bridge_pb.Metadata, error) {
	handler.rwmutex.RLock()
	defer handler.rwmutex.RUnlock()

	itemBytes, err := handler.backend.Get([]byte(MetadaKey))
	if err != nil {
		return nil, fmt.Errorf("%w, %v", ErrBackend, err)
	}

	if len(itemBytes) != 0 {
		item := &bridge_pb.Metadata{}
		if err := proto.Unmarshal(itemBytes, item); err != nil {
			return nil, fmt.Errorf("%w, %v", ErrDeserialization, err)
		}

		return item, nil
	}

	return nil, nil
}
