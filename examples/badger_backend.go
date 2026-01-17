// Example: BadgerDB Backend Integration
// ======================================
// This file shows how to integrate the composite key index with BadgerDB.
//
// To use this example, you need to install BadgerDB:
//   go get github.com/dgraph-io/badger/v4
//
// Build tags: badger

//go:build badger
// +build badger

package examples

import (
	"bytes"

	"github.com/dgraph-io/badger/v4"

	compositekey "composite_key_indexing"
)

// BadgerStore implements compositekey.KVStore interface using BadgerDB
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore creates a new BadgerDB-backed store
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable logging
	opts.Compression = badger.ZSTD
	opts.BlockCacheSize = 256 << 20 // 256MB
	opts.IndexCacheSize = 128 << 20 // 128MB
	
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	
	return &BadgerStore{db: db}, nil
}

// NewBadgerStoreInMemory creates an in-memory BadgerDB store
func NewBadgerStoreInMemory() (*BadgerStore, error) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	
	return &BadgerStore{db: db}, nil
}

// Get retrieves a value by key
func (s *BadgerStore) Get(key []byte) ([]byte, error) {
	var value []byte
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		
		value, err = item.ValueCopy(nil)
		return err
	})
	
	if err != nil {
		return nil, err
	}
	
	return value, nil
}

// Put stores a key-value pair
func (s *BadgerStore) Put(key []byte, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete removes a key
func (s *BadgerStore) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// RangeScan scans keys in range [startKey, endKey)
func (s *BadgerStore) RangeScan(startKey, endKey []byte, limit int) ([]compositekey.KVPair, error) {
	var results []compositekey.KVPair
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit
		
		iter := txn.NewIterator(opts)
		defer iter.Close()
		
		for iter.Seek(startKey); iter.Valid() && len(results) < limit; iter.Next() {
			item := iter.Item()
			key := item.KeyCopy(nil)
			
			if bytes.Compare(key, endKey) >= 0 {
				break
			}
			
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			
			results = append(results, compositekey.KVPair{
				Key:   key,
				Value: value,
			})
		}
		
		return nil
	})
	
	return results, err
}

// PrefixScan scans all keys with given prefix
func (s *BadgerStore) PrefixScan(prefix []byte, limit int) ([]compositekey.KVPair, error) {
	var results []compositekey.KVPair
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit
		opts.Prefix = prefix
		
		iter := txn.NewIterator(opts)
		defer iter.Close()
		
		for iter.Seek(prefix); iter.ValidForPrefix(prefix) && len(results) < limit; iter.Next() {
			item := iter.Item()
			key := item.KeyCopy(nil)
			
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			
			results = append(results, compositekey.KVPair{
				Key:   key,
				Value: value,
			})
		}
		
		return nil
	})
	
	return results, err
}

// Close closes the database
func (s *BadgerStore) Close() error {
	return s.db.Close()
}

// BatchWrite performs batch write using WriteBatch
func (s *BadgerStore) BatchWrite(pairs []compositekey.KVPair) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	
	for _, pair := range pairs {
		if err := wb.Set(pair.Key, pair.Value); err != nil {
			return err
		}
	}
	
	return wb.Flush()
}

// RunGC runs garbage collection
func (s *BadgerStore) RunGC() error {
	return s.db.RunValueLogGC(0.5)
}

// Example usage:
//
// func main() {
//     store, err := NewBadgerStore("/path/to/db")
//     if err != nil {
//         log.Fatal(err)
//     }
//     defer store.Close()
//
//     // Run GC periodically in production
//     go func() {
//         ticker := time.NewTicker(5 * time.Minute)
//         for range ticker.C {
//             store.RunGC()
//         }
//     }()
//
//     config := compositekey.IndexConfig{
//         Fields: []compositekey.FieldConfig{
//             {Name: "user_id", Type: compositekey.FieldTypeString, FixedLength: 32},
//             {Name: "timestamp", Type: compositekey.FieldTypeTimestamp},
//         },
//         EnableAntiHotspot:  true,
//         ShardCount:         256,
//         BloomFilterEnabled: true,
//         CacheEnabled:       true,
//     }
//
//     index := compositekey.NewCompositeKeyIndex(config, store)
//     
//     // Use index...
// }
