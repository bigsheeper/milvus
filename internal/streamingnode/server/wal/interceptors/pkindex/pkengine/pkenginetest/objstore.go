// Package pkenginetest provides shared test fixtures for the pkengine UTs.
package pkenginetest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
)

// MemObjectStore is an in-memory pkengine.ObjectStore used by the engine-layer
// UTs. It deliberately avoids internal/storage so the engine UTs stay free of
// the cgo/C++ transitive deps that storage.NewLocalChunkManager would pull in
// (report deviation D-OS1). Its method set mirrors storage.ChunkManager, so the
// production interceptor passes a real ChunkManager unchanged.
type MemObjectStore struct {
	mu   sync.Mutex
	root string
	m    map[string][]byte
}

var _ pkengine.ObjectStore = (*MemObjectStore)(nil)

// NewMemObjectStore creates an empty in-memory object store rooted at root.
func NewMemObjectStore(root string) *MemObjectStore {
	return &MemObjectStore{root: root, m: map[string][]byte{}}
}

func (s *MemObjectStore) RootPath() string { return s.root }

func (s *MemObjectStore) Write(_ context.Context, filePath string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[filePath] = append([]byte{}, content...)
	return nil
}

func (s *MemObjectStore) Read(_ context.Context, filePath string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[filePath]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", filePath)
	}
	return append([]byte{}, v...), nil
}

func (s *MemObjectStore) Exist(_ context.Context, filePath string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[filePath]
	return ok, nil
}

func (s *MemObjectStore) Remove(_ context.Context, filePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, filePath)
	return nil
}

// KeysWithSuffix returns the stored object keys matching suffix, sorted (test helper).
func (s *MemObjectStore) KeysWithSuffix(suffix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []string{}
	for k := range s.m {
		if strings.HasSuffix(k, suffix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Len returns the number of stored objects (test helper).
func (s *MemObjectStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
