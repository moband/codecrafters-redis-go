package store

import (
	"sync"
	"time"
)

// KeyValueStore holds key-value pairs with optional expiration
type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]valueWithExpiry
}

// valueWithExpiry holds a value and its expiration time
type valueWithExpiry struct {
	value    string
	expireAt time.Time // Zero time means no expiration
}

// NewKeyValueStore creates a new key-value store
func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]valueWithExpiry),
	}
}

// Set adds or updates a key-value pair with optional expiration
func (kv *KeyValueStore) Set(key, value string, expiry time.Duration) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var expireAt time.Time
	if expiry > 0 {
		expireAt = time.Now().Add(expiry)
	}

	kv.data[key] = valueWithExpiry{
		value:    value,
		expireAt: expireAt,
	}
}

// Get retrieves a value for a key, considering expiration
func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	entry, exists := kv.data[key]
	if !exists {
		return "", false
	}

	// Check if expired
	if !entry.expireAt.IsZero() && time.Now().After(entry.expireAt) {
		return "", false
	}

	return entry.value, true
}

// GetAllKeys returns all non-expired keys
func (kv *KeyValueStore) GetAllKeys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	keys := make([]string, 0, len(kv.data))

	for key, entry := range kv.data {
		if !entry.expireAt.IsZero() && time.Now().After(entry.expireAt) {
			continue
		}
		keys = append(keys, key)
	}

	return keys
}
