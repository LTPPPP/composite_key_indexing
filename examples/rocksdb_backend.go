// Example: RocksDB Backend Integration
// =====================================
// This file shows how to integrate the composite key index with RocksDB.
//
// To use this example, you need to install the RocksDB Go bindings:
//   go get github.com/linxGnu/grocksdb
//
// Build tags: rocksdb

//go:build rocksdb
// +build rocksdb

package examples

import (
	"bytes"

	"github.com/linxGnu/grocksdb"

	compositekey "composite_key_indexing"
)

// RocksDBStore implements compositekey.KVStore interface using RocksDB
type RocksDBStore struct {
	db       *grocksdb.DB
	readOpts *grocksdb.ReadOptions
	writeOpts *grocksdb.WriteOptions
}

// NewRocksDBStore creates a new RocksDB-backed store
func NewRocksDBStore(path string) (*RocksDBStore, error) {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(512 << 20)) // 512MB cache
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCompression(grocksdb.LZ4Compression)
	opts.SetWriteBufferSize(64 << 20) // 64MB
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(64 << 20)
	
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}
	
	return &RocksDBStore{
		db:       db,
		readOpts: grocksdb.NewDefaultReadOptions(),
		writeOpts: grocksdb.NewDefaultWriteOptions(),
	}, nil
}

// Get retrieves a value by key
func (s *RocksDBStore) Get(key []byte) ([]byte, error) {
	slice, err := s.db.Get(s.readOpts, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()
	
	if !slice.Exists() {
		return nil, nil
	}
	
	// Copy data since slice will be freed
	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	return data, nil
}

// Put stores a key-value pair
func (s *RocksDBStore) Put(key []byte, value []byte) error {
	return s.db.Put(s.writeOpts, key, value)
}

// Delete removes a key
func (s *RocksDBStore) Delete(key []byte) error {
	return s.db.Delete(s.writeOpts, key)
}

// RangeScan scans keys in range [startKey, endKey)
func (s *RocksDBStore) RangeScan(startKey, endKey []byte, limit int) ([]compositekey.KVPair, error) {
	iter := s.db.NewIterator(s.readOpts)
	defer iter.Close()
	
	var results []compositekey.KVPair
	
	for iter.Seek(startKey); iter.Valid() && len(results) < limit; iter.Next() {
		key := iter.Key()
		keyData := key.Data()
		
		if bytes.Compare(keyData, endKey) >= 0 {
			key.Free()
			break
		}
		
		value := iter.Value()
		
		// Copy data
		keyCopy := make([]byte, len(keyData))
		copy(keyCopy, keyData)
		valueCopy := make([]byte, len(value.Data()))
		copy(valueCopy, value.Data())
		
		results = append(results, compositekey.KVPair{
			Key:   keyCopy,
			Value: valueCopy,
		})
		
		key.Free()
		value.Free()
	}
	
	return results, iter.Err()
}

// PrefixScan scans all keys with given prefix
func (s *RocksDBStore) PrefixScan(prefix []byte, limit int) ([]compositekey.KVPair, error) {
	iter := s.db.NewIterator(s.readOpts)
	defer iter.Close()
	
	var results []compositekey.KVPair
	
	for iter.Seek(prefix); iter.Valid() && len(results) < limit; iter.Next() {
		key := iter.Key()
		keyData := key.Data()
		
		if !bytes.HasPrefix(keyData, prefix) {
			key.Free()
			break
		}
		
		value := iter.Value()
		
		// Copy data
		keyCopy := make([]byte, len(keyData))
		copy(keyCopy, keyData)
		valueCopy := make([]byte, len(value.Data()))
		copy(valueCopy, value.Data())
		
		results = append(results, compositekey.KVPair{
			Key:   keyCopy,
			Value: valueCopy,
		})
		
		key.Free()
		value.Free()
	}
	
	return results, iter.Err()
}

// Close closes the database
func (s *RocksDBStore) Close() {
	s.db.Close()
}

// BatchWrite performs batch write for better performance
func (s *RocksDBStore) BatchWrite(pairs []compositekey.KVPair) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	
	for _, pair := range pairs {
		batch.Put(pair.Key, pair.Value)
	}
	
	return s.db.Write(s.writeOpts, batch)
}

// Example usage:
//
// func main() {
//     store, err := NewRocksDBStore("/path/to/db")
//     if err != nil {
//         log.Fatal(err)
//     }
//     defer store.Close()
//
//     config := compositekey.IndexConfig{
//         Fields: []compositekey.FieldConfig{
//             {Name: "user_id", Type: compositekey.FieldTypeString, FixedLength: 32},
//             {Name: "timestamp", Type: compositekey.FieldTypeTimestamp},
//         },
//         EnableAntiHotspot:  true,
//         ShardCount:         256,
//         BloomFilterEnabled: false, // RocksDB has its own bloom filter
//         CacheEnabled:       false, // RocksDB has its own cache
//     }
//
//     index := compositekey.NewCompositeKeyIndex(config, store)
//     
//     // Use index...
// }
