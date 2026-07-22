package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

type noMetaStore struct {
	DefaultObjectStorage
	data map[string][]byte
}

func (s *noMetaStore) String() string { return "no-meta" }
func (s *noMetaStore) Put(_ context.Context, key string, in io.Reader, _ ...AttrGetter) error {
	b, _ := io.ReadAll(in)
	s.data[key] = b
	return nil
}
func (s *noMetaStore) Delete(_ context.Context, key string, _ ...AttrGetter) error {
	delete(s.data, key)
	return nil
}
func (s *noMetaStore) Head(_ context.Context, key string) (Object, error) {
	d, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &obj{key: key, size: int64(len(d))}, nil
}
func (s *noMetaStore) Get(_ context.Context, key string, off, limit int64, _ ...AttrGetter) (io.ReadCloser, error) {
	d, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return io.NopCloser(bytes.NewReader(d)), nil
}
func (s *noMetaStore) List(_ context.Context, prefix, _, _ string, _ string, _ int64, _ bool) ([]Object, bool, string, error) {
	var objs []Object
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			objs = append(objs, &obj{key: k, size: int64(len(s.data[k]))})
		}
	}
	return objs, false, "", nil
}

func TestPutWithMetaNoMetadataPutter(t *testing.T) {
	ctx := context.Background()
	底层 := &noMetaStore{data: make(map[string][]byte)}
	wrapped := WithPrefix(底层, "data/")

	mp, ok := wrapped.(MetadataPutter)
	if !ok {
		t.Fatal("wrapped 未实现 MetadataPutter")
	}

	err := mp.PutWithMeta(ctx, "file.txt", bytes.NewReader([]byte("hello")), ObjectMeta{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	// 修复后: key 应该是 "data/file.txt"，而不是 "data/data/file.txt"
	if _, ok := 底层.data["data/file.txt"]; !ok {
		t.Errorf("期望 key=\"data/file.txt\"，实际存储中的 key: %v", keys(底层.data))
	}
	if _, ok := 底层.data["data/data/file.txt"]; ok {
		t.Error("bug 未修复: 出现了 double-prefix key \"data/data/file.txt\"")
	}
}

func keys(m map[string][]byte) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
