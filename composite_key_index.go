// Package compositekey provides an optimized composite key indexing implementation
// for NoSQL, Key-Value Store, and Blockchain Ledger systems.
//
// Features:
//   - Byte-comparable key encoding
//   - Anti-hotspot shard prefix
//   - Bloom filter for negative lookups
//   - LRU cache with TTL
//   - Write buffer for batch commits
//   - Thread-safe operations
package compositekey

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// =============================================================================
// CONSTANTS & TYPES
// =============================================================================

// FieldType represents the data type of a field
type FieldType int

const (
	FieldTypeString FieldType = iota
	FieldTypeInteger
	FieldTypeTimestamp
	FieldTypeUUID
	FieldTypeDecimal
	FieldTypeBytes
)

// Default delimiter - Unit Separator (ASCII 31)
var DefaultDelimiter = []byte{0x1f}

// SignFlipMask for signed integer encoding
const SignFlipMask uint64 = 0x8000000000000000

// =============================================================================
// CONFIGURATION
// =============================================================================

// FieldConfig defines configuration for a single field
type FieldConfig struct {
	Name        string
	Type        FieldType
	FixedLength int // 0 = variable length
	Precision   int // For decimal type (default: 6)
}

// IndexConfig defines configuration for the composite key index
type IndexConfig struct {
	Fields             []FieldConfig
	Delimiter          []byte
	EnableAntiHotspot  bool
	ShardCount         int
	BloomFilterEnabled bool
	BloomExpectedItems int
	BloomFPRate        float64
	CacheEnabled       bool
	CacheMaxSize       int
	CacheTTLSeconds    int
	WriteBufferSize    int
}

// DefaultConfig returns a default configuration
func DefaultConfig() IndexConfig {
	return IndexConfig{
		Fields:             []FieldConfig{},
		Delimiter:          DefaultDelimiter,
		EnableAntiHotspot:  true,
		ShardCount:         256,
		BloomFilterEnabled: true,
		BloomExpectedItems: 1_000_000,
		BloomFPRate:        0.01,
		CacheEnabled:       true,
		CacheMaxSize:       10_000,
		CacheTTLSeconds:    300,
		WriteBufferSize:    10_000,
	}
}

// =============================================================================
// FIELD ENCODER
// =============================================================================

// FieldEncoder handles encoding/decoding of field values
type FieldEncoder struct{}

// Encode encodes a field value to bytes
func (e *FieldEncoder) Encode(value interface{}, config FieldConfig, delimiter []byte) ([]byte, error) {
	if value == nil {
		return []byte{0x00}, nil // NULL sorts first
	}

	switch config.Type {
	case FieldTypeString:
		return e.encodeString(value, config, delimiter)
	case FieldTypeInteger:
		return e.encodeInteger(value, config)
	case FieldTypeTimestamp:
		return e.encodeTimestamp(value, config)
	case FieldTypeUUID:
		return e.encodeUUID(value, config)
	case FieldTypeDecimal:
		return e.encodeDecimal(value, config)
	case FieldTypeBytes:
		return e.encodeBytes(value, config, delimiter)
	default:
		return nil, fmt.Errorf("unknown field type: %v", config.Type)
	}
}

func (e *FieldEncoder) encodeString(value interface{}, config FieldConfig, delimiter []byte) ([]byte, error) {
	var str string
	switch v := value.(type) {
	case string:
		str = v
	case []byte:
		str = string(v)
	default:
		str = fmt.Sprintf("%v", v)
	}

	encoded := []byte(str)

	// Escape delimiter (double it)
	escaped := bytes.ReplaceAll(encoded, delimiter, append(delimiter, delimiter...))

	// Apply fixed length if configured
	if config.FixedLength > 0 {
		if len(escaped) > config.FixedLength {
			escaped = escaped[:config.FixedLength]
		} else {
			padding := make([]byte, config.FixedLength-len(escaped))
			escaped = append(escaped, padding...)
		}
	}

	return escaped, nil
}

func (e *FieldEncoder) encodeInteger(value interface{}, config FieldConfig) ([]byte, error) {
	var num int64
	switch v := value.(type) {
	case int:
		num = int64(v)
	case int32:
		num = int64(v)
	case int64:
		num = v
	case uint:
		num = int64(v)
	case uint32:
		num = int64(v)
	case uint64:
		num = int64(v)
	default:
		return nil, fmt.Errorf("cannot encode %T as integer", value)
	}

	// XOR with sign bit to maintain sort order for negative numbers
	var adjusted uint64
	if num >= 0 {
		adjusted = uint64(num) | SignFlipMask
	} else {
		adjusted = uint64(num) ^ SignFlipMask
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, adjusted)
	return buf, nil
}

func (e *FieldEncoder) encodeTimestamp(value interface{}, config FieldConfig) ([]byte, error) {
	var ts uint64
	switch v := value.(type) {
	case int64:
		if v < 0 {
			return nil, errors.New("timestamp cannot be negative")
		}
		ts = uint64(v)
	case uint64:
		ts = v
	case time.Time:
		ts = uint64(v.UnixMilli())
	default:
		return nil, fmt.Errorf("cannot encode %T as timestamp", value)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, ts)
	return buf, nil
}

func (e *FieldEncoder) encodeUUID(value interface{}, config FieldConfig) ([]byte, error) {
	switch v := value.(type) {
	case string:
		u, err := uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID string: %w", err)
		}
		return u[:], nil
	case uuid.UUID:
		return v[:], nil
	case [16]byte:
		return v[:], nil
	case []byte:
		if len(v) != 16 {
			return nil, fmt.Errorf("UUID must be 16 bytes, got %d", len(v))
		}
		return v, nil
	default:
		return nil, fmt.Errorf("cannot encode %T as UUID", value)
	}
}

func (e *FieldEncoder) encodeDecimal(value interface{}, config FieldConfig) ([]byte, error) {
	var num float64
	switch v := value.(type) {
	case float32:
		num = float64(v)
	case float64:
		num = v
	default:
		return nil, fmt.Errorf("cannot encode %T as decimal", value)
	}

	precision := config.Precision
	if precision == 0 {
		precision = 6 // default
	}

	// Scale to integer
	scale := 1.0
	for i := 0; i < precision; i++ {
		scale *= 10
	}
	scaled := int64(num * scale)

	return e.encodeInteger(scaled, config)
}

func (e *FieldEncoder) encodeBytes(value interface{}, config FieldConfig, delimiter []byte) ([]byte, error) {
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return nil, fmt.Errorf("cannot encode %T as bytes", value)
	}

	// Escape delimiter
	escaped := bytes.ReplaceAll(data, delimiter, append(delimiter, delimiter...))

	// Apply fixed length
	if config.FixedLength > 0 {
		if len(escaped) > config.FixedLength {
			escaped = escaped[:config.FixedLength]
		} else {
			padding := make([]byte, config.FixedLength-len(escaped))
			escaped = append(escaped, padding...)
		}
	}

	return escaped, nil
}

// Decode decodes bytes back to a field value
func (e *FieldEncoder) Decode(encoded []byte, config FieldConfig, delimiter []byte) (interface{}, error) {
	if len(encoded) == 1 && encoded[0] == 0x00 {
		return nil, nil
	}

	switch config.Type {
	case FieldTypeString:
		return e.decodeString(encoded, delimiter), nil
	case FieldTypeInteger:
		return e.decodeInteger(encoded), nil
	case FieldTypeTimestamp:
		return e.decodeTimestamp(encoded), nil
	case FieldTypeUUID:
		return e.decodeUUID(encoded), nil
	case FieldTypeDecimal:
		return e.decodeDecimal(encoded, config), nil
	case FieldTypeBytes:
		return e.decodeBytes(encoded, delimiter), nil
	default:
		return nil, fmt.Errorf("unknown field type: %v", config.Type)
	}
}

func (e *FieldEncoder) decodeString(encoded []byte, delimiter []byte) string {
	// Unescape delimiter
	unescaped := bytes.ReplaceAll(encoded, append(delimiter, delimiter...), delimiter)
	// Remove padding
	unescaped = bytes.TrimRight(unescaped, "\x00")
	return string(unescaped)
}

func (e *FieldEncoder) decodeInteger(encoded []byte) int64 {
	value := binary.BigEndian.Uint64(encoded)

	// Reverse sign flip
	if value&SignFlipMask != 0 {
		return int64(value ^ SignFlipMask)
	}
	return int64(value ^ SignFlipMask)
}

func (e *FieldEncoder) decodeTimestamp(encoded []byte) int64 {
	return int64(binary.BigEndian.Uint64(encoded))
}

func (e *FieldEncoder) decodeUUID(encoded []byte) string {
	u, _ := uuid.FromBytes(encoded)
	return u.String()
}

func (e *FieldEncoder) decodeDecimal(encoded []byte, config FieldConfig) float64 {
	scaled := e.decodeInteger(encoded)
	precision := config.Precision
	if precision == 0 {
		precision = 6
	}

	scale := 1.0
	for i := 0; i < precision; i++ {
		scale *= 10
	}
	return float64(scaled) / scale
}

func (e *FieldEncoder) decodeBytes(encoded []byte, delimiter []byte) []byte {
	unescaped := bytes.ReplaceAll(encoded, append(delimiter, delimiter...), delimiter)
	return bytes.TrimRight(unescaped, "\x00")
}

// =============================================================================
// COMPOSITE KEY BUILDER
// =============================================================================

// CompositeKeyBuilder builds composite keys from field values
type CompositeKeyBuilder struct {
	config   IndexConfig
	encoder  *FieldEncoder
	fieldMap map[string]FieldConfig
}

// NewCompositeKeyBuilder creates a new key builder
func NewCompositeKeyBuilder(config IndexConfig) *CompositeKeyBuilder {
	fieldMap := make(map[string]FieldConfig)
	for _, f := range config.Fields {
		fieldMap[f.Name] = f
	}

	return &CompositeKeyBuilder{
		config:   config,
		encoder:  &FieldEncoder{},
		fieldMap: fieldMap,
	}
}

// CreateKey creates a composite key from field values
func (b *CompositeKeyBuilder) CreateKey(values map[string]interface{}) ([]byte, error) {
	var keyParts [][]byte

	// Add anti-hotspot prefix if enabled
	if b.config.EnableAntiHotspot && len(b.config.Fields) > 0 {
		firstField := b.config.Fields[0]
		if firstValue, ok := values[firstField.Name]; ok && firstValue != nil {
			shardPrefix := b.computeShardPrefix(firstValue)
			keyParts = append(keyParts, shardPrefix)
		}
	}

	// Encode each field in order
	for _, fieldConfig := range b.config.Fields {
		value, ok := values[fieldConfig.Name]
		if !ok || value == nil {
			break // Stop at first missing field
		}

		encoded, err := b.encoder.Encode(value, fieldConfig, b.config.Delimiter)
		if err != nil {
			return nil, fmt.Errorf("failed to encode field %s: %w", fieldConfig.Name, err)
		}
		keyParts = append(keyParts, encoded)
	}

	// Join with delimiter
	return bytes.Join(keyParts, b.config.Delimiter), nil
}

// CreateRangeKeys creates start and end keys for range queries
func (b *CompositeKeyBuilder) CreateRangeKeys(
	equalityValues map[string]interface{},
	rangeField string,
	minValue interface{},
	maxValue interface{},
) (startKey []byte, endKey []byte, err error) {
	// Start key
	startValues := make(map[string]interface{})
	for k, v := range equalityValues {
		startValues[k] = v
	}
	startValues[rangeField] = minValue
	startKey, err = b.CreateKey(startValues)
	if err != nil {
		return nil, nil, err
	}

	// End key (inclusive)
	endValues := make(map[string]interface{})
	for k, v := range equalityValues {
		endValues[k] = v
	}
	endValues[rangeField] = maxValue
	endKey, err = b.CreateKey(endValues)
	if err != nil {
		return nil, nil, err
	}
	endKey = IncrementKey(endKey)

	return startKey, endKey, nil
}

func (b *CompositeKeyBuilder) computeShardPrefix(value interface{}) []byte {
	// Convert value to bytes for hashing
	var valueBytes []byte
	switch v := value.(type) {
	case string:
		valueBytes = []byte(v)
	case []byte:
		valueBytes = v
	default:
		valueBytes = []byte(fmt.Sprintf("%v", v))
	}

	// Hash and mod by shard count
	hash := md5.Sum(valueBytes)
	hashInt := binary.BigEndian.Uint64(hash[:8])
	shardID := hashInt % uint64(b.config.ShardCount)

	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(shardID))
	return buf
}

// IncrementKey increments the last byte of a key
func IncrementKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{0x00}
	}

	result := make([]byte, len(key))
	copy(result, key)

	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result[:i+1]
		}
	}

	// All bytes are 0xFF
	return append(key, 0x00)
}

// =============================================================================
// BLOOM FILTER
// =============================================================================

// BloomFilter provides probabilistic existence checking
type BloomFilter struct {
	bitArray  []byte
	size      uint64
	hashCount int
	mu        sync.RWMutex
}

// NewBloomFilter creates a new bloom filter
func NewBloomFilter(expectedItems int, fpRate float64) *BloomFilter {
	import_math := func() float64 { return 2.302585 } // ln(10) approximation
	_ = import_math

	// Calculate optimal size: -n * ln(p) / (ln(2)^2)
	ln2 := 0.693147
	size := uint64(float64(-expectedItems) * (-2.302585 - float64(expectedItems)*0.00001) / (ln2 * ln2))
	if size < 1000 {
		size = 1000
	}

	// Calculate optimal hash count: m/n * ln(2)
	hashCount := int(float64(size) / float64(expectedItems) * ln2)
	if hashCount < 1 {
		hashCount = 1
	}
	if hashCount > 20 {
		hashCount = 20
	}

	return &BloomFilter{
		bitArray:  make([]byte, (size+7)/8),
		size:      size,
		hashCount: hashCount,
	}
}

func (bf *BloomFilter) hash(item []byte, seed int) uint64 {
	h := fnv.New64a()
	h.Write(item)
	h.Write([]byte{byte(seed)})
	return h.Sum64() % bf.size
}

// Add adds an item to the filter
func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := 0; i < bf.hashCount; i++ {
		pos := bf.hash(item, i)
		byteIdx := pos / 8
		bitIdx := pos % 8
		bf.bitArray[byteIdx] |= 1 << bitIdx
	}
}

// MightContain checks if item might be in the filter
func (bf *BloomFilter) MightContain(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := 0; i < bf.hashCount; i++ {
		pos := bf.hash(item, i)
		byteIdx := pos / 8
		bitIdx := pos % 8
		if bf.bitArray[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

// =============================================================================
// LRU CACHE
// =============================================================================

// CacheEntry holds cached data with timestamp
type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
}

// LRUCache provides thread-safe LRU caching with TTL
type LRUCache struct {
	maxSize int
	ttl     time.Duration
	cache   map[string]*CacheEntry
	order   []string // For LRU tracking
	mu      sync.RWMutex
}

// NewLRUCache creates a new LRU cache
func NewLRUCache(maxSize int, ttlSeconds int) *LRUCache {
	return &LRUCache{
		maxSize: maxSize,
		ttl:     time.Duration(ttlSeconds) * time.Second,
		cache:   make(map[string]*CacheEntry),
		order:   make([]string, 0),
	}
}

// Get retrieves an item from cache
func (c *LRUCache) Get(key []byte) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key)
	entry, ok := c.cache[keyStr]
	if !ok {
		return nil, false
	}

	// Check TTL
	if time.Since(entry.Timestamp) > c.ttl {
		delete(c.cache, keyStr)
		c.removeFromOrder(keyStr)
		return nil, false
	}

	// Move to end (most recently used)
	c.removeFromOrder(keyStr)
	c.order = append(c.order, keyStr)

	return entry.Data, true
}

// Put adds an item to cache
func (c *LRUCache) Put(key []byte, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key)

	// Remove if exists
	if _, ok := c.cache[keyStr]; ok {
		c.removeFromOrder(keyStr)
	}

	// Add new entry
	c.cache[keyStr] = &CacheEntry{
		Data:      value,
		Timestamp: time.Now(),
	}
	c.order = append(c.order, keyStr)

	// Evict if over capacity
	for len(c.cache) > c.maxSize && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.cache, oldest)
	}
}

// InvalidatePrefix invalidates all entries matching prefix
func (c *LRUCache) InvalidatePrefix(prefix []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	prefixStr := string(prefix)
	count := 0

	for key := range c.cache {
		if len(key) >= len(prefixStr) && key[:len(prefixStr)] == prefixStr {
			delete(c.cache, key)
			c.removeFromOrder(key)
			count++
		}
	}

	return count
}

func (c *LRUCache) removeFromOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// Clear clears the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*CacheEntry)
	c.order = make([]string, 0)
}

// =============================================================================
// KV STORE INTERFACE
// =============================================================================

// KVStore defines the interface for key-value storage
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	RangeScan(startKey, endKey []byte, limit int) ([]KVPair, error)
	PrefixScan(prefix []byte, limit int) ([]KVPair, error)
}

// KVPair represents a key-value pair
type KVPair struct {
	Key   []byte
	Value []byte
}

// InMemoryKVStore provides an in-memory sorted KV store for testing
type InMemoryKVStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewInMemoryKVStore creates a new in-memory store
func NewInMemoryKVStore() *InMemoryKVStore {
	return &InMemoryKVStore{
		data: make(map[string][]byte),
	}
}

// Get retrieves a value
func (s *InMemoryKVStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[string(key)]
	if !ok {
		return nil, nil
	}
	return value, nil
}

// Put stores a value
func (s *InMemoryKVStore) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[string(key)] = value
	return nil
}

// Delete removes a key
func (s *InMemoryKVStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, string(key))
	return nil
}

// RangeScan scans keys in range [startKey, endKey)
func (s *InMemoryKVStore) RangeScan(startKey, endKey []byte, limit int) ([]KVPair, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get all keys and sort
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sortStrings(keys)

	var results []KVPair
	for _, k := range keys {
		if len(results) >= limit {
			break
		}
		keyBytes := []byte(k)
		if bytes.Compare(keyBytes, startKey) >= 0 && bytes.Compare(keyBytes, endKey) < 0 {
			results = append(results, KVPair{
				Key:   keyBytes,
				Value: s.data[k],
			})
		}
	}

	return results, nil
}

// PrefixScan scans all keys with given prefix
func (s *InMemoryKVStore) PrefixScan(prefix []byte, limit int) ([]KVPair, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sortStrings(keys)

	var results []KVPair
	for _, k := range keys {
		if len(results) >= limit {
			break
		}
		if bytes.HasPrefix([]byte(k), prefix) {
			results = append(results, KVPair{
				Key:   []byte(k),
				Value: s.data[k],
			})
		}
	}

	return results, nil
}

// sortStrings using built-in sort
func sortStrings(strs []string) {
	sort.Strings(strs)
}

// =============================================================================
// WRITE BUFFER
// =============================================================================

// WriteBuffer batches writes for better performance
type WriteBuffer struct {
	store   KVStore
	buffer  map[string][]byte
	maxSize int
	mu      sync.Mutex
}

// NewWriteBuffer creates a new write buffer
func NewWriteBuffer(store KVStore, maxSize int) *WriteBuffer {
	return &WriteBuffer{
		store:   store,
		buffer:  make(map[string][]byte),
		maxSize: maxSize,
	}
}

// Put adds to buffer, auto-flushes if full
func (wb *WriteBuffer) Put(key []byte, value []byte) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.buffer[string(key)] = value

	if len(wb.buffer) >= wb.maxSize {
		_, err := wb.flushUnlocked()
		return err
	}
	return nil
}

// Get retrieves from buffer first, then store
func (wb *WriteBuffer) Get(key []byte) ([]byte, error) {
	wb.mu.Lock()
	if value, ok := wb.buffer[string(key)]; ok {
		wb.mu.Unlock()
		return value, nil
	}
	wb.mu.Unlock()

	return wb.store.Get(key)
}

// Flush writes buffer to store
func (wb *WriteBuffer) Flush() (int, error) {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	return wb.flushUnlocked()
}

func (wb *WriteBuffer) flushUnlocked() (int, error) {
	count := len(wb.buffer)

	for key, value := range wb.buffer {
		if err := wb.store.Put([]byte(key), value); err != nil {
			return 0, err
		}
	}

	wb.buffer = make(map[string][]byte)
	return count, nil
}

// =============================================================================
// QUERY RESULT
// =============================================================================

// QueryResult contains the results of a query
type QueryResult struct {
	Records   []map[string]interface{}
	HasMore   bool
	ScanCount int
	CacheHit  bool
}

// =============================================================================
// COMPOSITE KEY INDEX
// =============================================================================

// CompositeKeyIndex provides the main index implementation
type CompositeKeyIndex struct {
	config      IndexConfig
	keyBuilder  *CompositeKeyBuilder
	store       KVStore
	writeBuffer *WriteBuffer
	bloomFilter *BloomFilter
	cache       *LRUCache
}

// NewCompositeKeyIndex creates a new composite key index
func NewCompositeKeyIndex(config IndexConfig, store KVStore) *CompositeKeyIndex {
	if store == nil {
		store = NewInMemoryKVStore()
	}

	idx := &CompositeKeyIndex{
		config:      config,
		keyBuilder:  NewCompositeKeyBuilder(config),
		store:       store,
		writeBuffer: NewWriteBuffer(store, config.WriteBufferSize),
	}

	if config.BloomFilterEnabled {
		idx.bloomFilter = NewBloomFilter(config.BloomExpectedItems, config.BloomFPRate)
	}

	if config.CacheEnabled {
		idx.cache = NewLRUCache(config.CacheMaxSize, config.CacheTTLSeconds)
	}

	return idx
}

// =============================================================================
// WRITE OPERATIONS
// =============================================================================

// Put inserts or updates a record
func (idx *CompositeKeyIndex) Put(record map[string]interface{}) ([]byte, error) {
	// Build composite key
	key, err := idx.keyBuilder.CreateKey(record)
	if err != nil {
		return nil, err
	}

	// Serialize record
	value, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}

	// Write to buffer
	if err := idx.writeBuffer.Put(key, value); err != nil {
		return nil, err
	}

	// Update bloom filter
	if idx.bloomFilter != nil {
		idx.bloomFilter.Add(key)
	}

	// Invalidate cache
	if idx.cache != nil {
		idx.cache.InvalidatePrefix(key)
	}

	return key, nil
}

// Delete removes a record
func (idx *CompositeKeyIndex) Delete(values map[string]interface{}) error {
	key, err := idx.keyBuilder.CreateKey(values)
	if err != nil {
		return err
	}

	// Flush buffer first
	idx.writeBuffer.Flush()

	// Delete from store
	if err := idx.store.Delete(key); err != nil {
		return err
	}

	// Invalidate cache
	if idx.cache != nil {
		idx.cache.InvalidatePrefix(key)
	}

	return nil
}

// Flush writes buffered data to storage
func (idx *CompositeKeyIndex) Flush() (int, error) {
	return idx.writeBuffer.Flush()
}

// =============================================================================
// READ OPERATIONS
// =============================================================================

// ExactLookup performs exact match lookup
func (idx *CompositeKeyIndex) ExactLookup(values map[string]interface{}) (map[string]interface{}, error) {
	key, err := idx.keyBuilder.CreateKey(values)
	if err != nil {
		return nil, err
	}

	// Check bloom filter
	if idx.bloomFilter != nil && !idx.bloomFilter.MightContain(key) {
		return nil, nil
	}

	// Check cache
	if idx.cache != nil {
		if cached, ok := idx.cache.Get(key); ok {
			if record, ok := cached.(map[string]interface{}); ok {
				return record, nil
			}
		}
	}

	// Lookup in store
	value, err := idx.writeBuffer.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	// Deserialize
	var record map[string]interface{}
	if err := json.Unmarshal(value, &record); err != nil {
		return nil, err
	}

	// Update cache
	if idx.cache != nil {
		idx.cache.Put(key, record)
	}

	return record, nil
}

// PrefixScan performs prefix scan for partial match
func (idx *CompositeKeyIndex) PrefixScan(values map[string]interface{}, limit int) (*QueryResult, error) {
	// Flush buffer
	idx.writeBuffer.Flush()

	prefix, err := idx.keyBuilder.CreateKey(values)
	if err != nil {
		return nil, err
	}

	// Check cache
	cacheKey := append(prefix, []byte(":prefix")...)
	if idx.cache != nil {
		if cached, ok := idx.cache.Get(cacheKey); ok {
			if records, ok := cached.([]map[string]interface{}); ok {
				result := records
				if len(result) > limit {
					result = result[:limit]
				}
				return &QueryResult{
					Records:   result,
					HasMore:   len(records) > limit,
					ScanCount: 0,
					CacheHit:  true,
				}, nil
			}
		}
	}

	// Execute scan
	pairs, err := idx.store.PrefixScan(prefix, limit+1)
	if err != nil {
		return nil, err
	}

	records := make([]map[string]interface{}, 0, len(pairs))
	for _, pair := range pairs {
		var record map[string]interface{}
		if err := json.Unmarshal(pair.Value, &record); err != nil {
			continue
		}
		records = append(records, record)
	}

	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	// Update cache
	if idx.cache != nil && !hasMore {
		idx.cache.Put(cacheKey, records)
	}

	return &QueryResult{
		Records:   records,
		HasMore:   hasMore,
		ScanCount: len(pairs),
	}, nil
}

// RangeScan performs range scan with equality prefix
func (idx *CompositeKeyIndex) RangeScan(
	equalityValues map[string]interface{},
	rangeField string,
	minValue interface{},
	maxValue interface{},
	limit int,
) (*QueryResult, error) {
	// Flush buffer
	idx.writeBuffer.Flush()

	startKey, endKey, err := idx.keyBuilder.CreateRangeKeys(
		equalityValues,
		rangeField,
		minValue,
		maxValue,
	)
	if err != nil {
		return nil, err
	}

	// Execute scan
	pairs, err := idx.store.RangeScan(startKey, endKey, limit+1)
	if err != nil {
		return nil, err
	}

	records := make([]map[string]interface{}, 0, len(pairs))
	for _, pair := range pairs {
		var record map[string]interface{}
		if err := json.Unmarshal(pair.Value, &record); err != nil {
			continue
		}
		records = append(records, record)
	}

	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	return &QueryResult{
		Records:   records,
		HasMore:   hasMore,
		ScanCount: len(pairs),
	}, nil
}
