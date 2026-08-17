/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sync

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	sync_db "github.com/juicedata/juicefs/pkg/sync/db"
)

func collectAll(c <-chan object.Object) []string {
	r := make([]string, 0)
	for s := range c {
		r = append(r, s.Key())
	}
	return r
}

// nolint:errcheck
func TestIterator(t *testing.T) {
	m, _ := object.CreateStorage("mem", "", "", "", "")
	m.Put(ctx, "a", bytes.NewReader([]byte("a")))
	m.Put(ctx, "b", bytes.NewReader([]byte("a")))
	m.Put(ctx, "aa", bytes.NewReader([]byte("a")))
	m.Put(ctx, "c", bytes.NewReader([]byte("a")))

	ch, _ := ListAll(m, "", "a", "b", true)
	keys := collectAll(ch)
	if len(keys) != 3 {
		t.Fatalf("length should be 3, but got %d", len(keys))
	}
	if !reflect.DeepEqual(keys, []string{"a", "aa", "b"}) {
		t.Fatalf("result wrong: %s", keys)
	}

	// Single object
	s, _ := object.CreateStorage("mem", "", "", "", "")
	s.Put(ctx, "a", bytes.NewReader([]byte("a")))
	ch, _ = ListAll(s, "", "", "", true)
	keys = collectAll(ch)
	if !reflect.DeepEqual(keys, []string{"a"}) {
		t.Fatalf("result wrong: %s", keys)
	}
}

func TestIeratorSingleEmptyKey(t *testing.T) {
	// utils.SetLogLevel(logrus.DebugLevel)

	// Construct mem storage
	s, _ := object.CreateStorage("mem", "", "", "", "")
	err := s.Put(ctx, "abc", bytes.NewReader([]byte("abc")))
	if err != nil {
		t.Fatalf("Put error: %q", err)
	}

	// Simulate command line prefix in SRC or DST
	s = object.WithPrefix(s, "abc")
	ch, _ := ListAll(s, "", "", "", true)
	keys := collectAll(ch)
	if !reflect.DeepEqual(keys, []string{""}) {
		t.Fatalf("result wrong: %s", keys)
	}
}

func deepEqualWithOutMtime(a, b object.Object) bool {
	return a.IsDir() == b.IsDir() && a.Key() == b.Key() && a.Size() == b.Size() &&
		math.Abs(a.Mtime().Sub(b.Mtime()).Seconds()) < 1
}

// nolint:errcheck
func TestSync(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()
	config := &Config{
		Start:       "",
		End:         "",
		Threads:     50,
		ListThreads: 1,
		Update:      true,
		Perms:       true,
		Dry:         false,
		DeleteSrc:   false,
		Limit:       -1,
		DeleteDst:   false,
		Exclude:     []string{"c*"},
		Include:     []string{"a[1-9]", "a*"},
		MaxSize:     math.MaxInt64,
		Verbose:     false,
		Quiet:       true,
	}
	os.Args = []string{"--include", "a[1-9]", "--exclude", "a*", "--exclude", "c*"}
	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	a.Put(ctx, "a1", bytes.NewReader([]byte("a1")))
	a.Put(ctx, "a2", bytes.NewReader([]byte("a2")))
	a.Put(ctx, "abc", bytes.NewReader([]byte("abc")))
	a.Put(ctx, "c1", bytes.NewReader([]byte("c1")))
	a.Put(ctx, "c2", bytes.NewReader([]byte("c2")))

	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")
	b.Put(ctx, "a1", bytes.NewReader([]byte("a1")))
	b.Put(ctx, "ba", bytes.NewReader([]byte("a1")))

	// Copy a2
	if err := Sync(a, b, config); err != nil {
		t.Fatalf("sync: %s", err)
	}
	if c := copied.Current(); c != 1 {
		t.Fatalf("should copy 1 keys, but got %d", c)
	}

	if err := Sync(a, b, config); err != nil {
		t.Fatalf("sync: %s", err)
	}
	// No copy occurred
	if c := copied.Current(); c != 0 {
		t.Fatalf("should copy 0 keys, but got %d", c)
	}

	// Now a: {"a1", "a2", "abc", "c1", "c2"}, b: {"a1", "a2", "ba"}
	// Copy "ba" from b to a
	os.Args = []string{}
	config.Exclude = nil
	config.rules = nil
	if err := Sync(b, a, config); err != nil {
		t.Fatalf("sync: %s", err)
	}
	if c := copied.Current(); c != 1 {
		t.Fatalf("should copy 1 keys, but got %d", c)
	}
	// Now a: {"a1", "a2", "abc", "ba", "c1", "c2"}, b: {"a1", "a2", "ba"}
	aRes, _ := ListAll(a, "", "", "", true)
	bRes, _ := ListAll(b, "", "", "", true)

	var aObjs, bObjs []object.Object
	for obj := range aRes {
		aObjs = append(aObjs, obj)
	}
	for obj := range bRes {
		bObjs = append(bObjs, obj)
	}

	if !deepEqualWithOutMtime(aObjs[1], bObjs[1]) {
		t.FailNow()
	}

	if !deepEqualWithOutMtime(aObjs[4], bObjs[len(bObjs)-1]) {
		t.Fatalf("expect %+v but got %+v", aObjs[4], bObjs[len(bObjs)-1])
	}
	// Test --force-update option
	config.ForceUpdate = true
	// Forcibly copy {"a1", "a2", "abc","c1","c2","ba"} from a to b.
	if err := Sync(a, b, config); err != nil {
		t.Fatalf("sync: %s", err)
	}
}

// nolint:errcheck
func TestSyncIncludeAndExclude(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()
	config := &Config{
		Start:       "",
		End:         "",
		Threads:     50,
		ListThreads: 1,
		Update:      true,
		Perms:       true,
		Dry:         false,
		DeleteSrc:   false,
		DeleteDst:   false,
		Verbose:     false,
		Limit:       -1,
		Quiet:       true,
		MaxSize:     math.MaxInt64,
		Exclude:     []string{"1"},
	}
	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")

	simple := []string{"a1/z1/z2", "a2", "ab1", "ab2", "b1", "b2", "c1", "c2"}
	testCases := []struct {
		srcKey, args, want []string
	}{
		{
			srcKey: simple,
			args:   []string{"--include", "xx*", "--include", "xxx*"},
			want:   []string{"a1/", "a1/z1/", "a1/z1/z2", "a2", "ab1", "ab2", "b1", "b2", "c1", "c2"},
		},
		{
			srcKey: simple,
			args:   []string{"--exclude", "a*", "--exclude", "c*"},
			want:   []string{"b1", "b2"},
		},
		{
			srcKey: simple,
			args:   []string{"--exclude", "a[1-2]", "--include", "a*"},
			want:   []string{"ab1", "ab2", "b1", "b2", "c1", "c2"},
		},
		{
			srcKey: simple,
			args:   []string{"--exclude", "ab?", "--include", "a*"},
			want:   []string{"a1/", "a1/z1/", "a1/z1/z2", "a2", "b1", "b2", "c1", "c2"},
		},
		{
			srcKey: simple,
			args:   []string{"--include", "a*", "--exclude", "c*"},
			want:   []string{"a1/", "a1/z1/", "a1/z1/z2", "a2", "ab1", "ab2", "b1", "b2"},
		},
		{
			srcKey: simple,
			args:   []string{"--exclude", "a*", "--exclude", "c*"},
			want:   []string{"b1", "b2"},
		},
		{
			srcKey: []string{"a1/b1/c1", "a1/b1/c2", "a1/b2/c1", "a1/b2/c2", "a2/b1/c2", "a3/b2/c2", "a4"},
			args:   []string{"--exclude", "a*/b[1-2]/c1", "--exclude", "a4"},
			want:   []string{"a1/", "a1/b1/", "a1/b1/c2", "a1/b2/", "a1/b2/c2", "a2/", "a2/b1/", "a2/b1/c2", "a3/", "a3/b2/", "a3/b2/c2"},
		},
	}

	for _, testCase := range testCases {
		_ = os.RemoveAll("/tmp/a/")
		_ = os.RemoveAll("/tmp/b/")
		os.Args = testCase.args
		for _, k := range testCase.srcKey {
			a.Put(ctx, k, bytes.NewReader([]byte(k)))
		}
		if err := Sync(a, b, config); err != nil {
			t.Fatalf("sync: %s", err)
		}

		bRes, _ := ListAll(b, "", "", "", true)
		var bKeys []string
		for obj := range bRes {
			bKeys = append(bKeys, obj.Key())
		}
		if !reflect.DeepEqual(bKeys[1:], testCase.want) {
			t.Errorf("sync args  %v, want %v, but get %v", os.Args, testCase.want, bKeys)
		}
	}
}

func TestParseRules(t *testing.T) {
	tests := []struct {
		args      []string
		wantRules []rule
	}{
		{
			args:      []string{"--include", "a"},
			wantRules: []rule{{pattern: "a", include: true}},
		},
		{
			args:      []string{"--exclude", "a", "--include", "b"},
			wantRules: []rule{{pattern: "a"}, {pattern: "b", include: true}},
		},
		{
			args:      []string{"--include", "a", "--test", "t", "--exclude", "b"},
			wantRules: []rule{{pattern: "a", include: true}, {pattern: "b"}},
		},
		{
			args:      []string{"--include", "a", "--test", "t", "--exclude"},
			wantRules: []rule{{pattern: "a", include: true}},
		},
		{
			args:      []string{"--include", "a", "--exclude", "b", "--include", "c", "--exclude", "d"},
			wantRules: []rule{{pattern: "a", include: true}, {pattern: "b"}, {pattern: "c", include: true}, {pattern: "d"}},
		},
		{
			args:      []string{"--include", "a", "--include", "b", "--test", "--exclude", "c", "--exclude", "d"},
			wantRules: []rule{{pattern: "a", include: true}, {pattern: "b", include: true}, {pattern: "c"}, {pattern: "d"}},
		},
		{
			args:      []string{"--include=a", "--include=b", "--exclude=c", "--exclude=d", "--test=aaa"},
			wantRules: []rule{{pattern: "a", include: true}, {pattern: "b", include: true}, {pattern: "c"}, {pattern: "d"}},
		},
		{
			args:      []string{"-include=a", "--test", "t", "--include=b", "--exclude=c", "-exclude="},
			wantRules: []rule{{pattern: "a", include: true}, {pattern: "b", include: true}, {pattern: "c"}},
		},
	}
	for _, tt := range tests {
		if gotRules := parseIncludeRules(tt.args); !reflect.DeepEqual(gotRules, tt.wantRules) {
			t.Errorf("got %+v, want %+v", gotRules, tt.wantRules)
		}
	}
}

func TestSyncLink(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()

	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	a.Put(ctx, "a1", bytes.NewReader([]byte("test")))
	as := a.(object.SupportSymlink)
	as.Symlink("/tmp/a/a1", "l1")
	as.Symlink("./../a1", "d1/l2")
	as.Symlink("./../notExist", "l3")

	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")
	bs := b.(object.SupportSymlink)
	bs.Symlink("/tmp/b/a1", "l1")

	if err := Sync(a, b, &Config{
		Threads:     50,
		Update:      true,
		Perms:       true,
		ListThreads: 1,
		Links:       true,
		Quiet:       true,
		Limit:       -1,
		ForceUpdate: true,
		MaxSize:     math.MaxInt64,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}

	l1, err := bs.Readlink("l1")
	if err != nil || l1 != "/tmp/a/a1" {
		t.Fatalf("readlink: %s content: %s", err, l1)
	}
	content, err := b.Get(ctx, "l1", 0, -1)
	if err != nil {
		t.Fatalf("get content failed: %s", err)
	}
	if c, err := io.ReadAll(content); err != nil || string(c) != "test" {
		t.Fatalf("read content failed: err %s content %s", err, string(c))
	}

	l2, err := bs.Readlink("d1/l2")
	if err != nil || l2 != "./../a1" {
		t.Fatalf("readlink: %s", err)
	}
	content, err = b.Get(ctx, "d1/l2", 0, -1)
	if err != nil {
		t.Fatalf("content failed: %s", err)
	}
	if c, err := io.ReadAll(content); err != nil || string(c) != "test" {
		t.Fatalf("read content failed: err %s content %s", err, string(c))
	}

	l3, err := bs.Readlink("l3")
	if err != nil || l3 != "./../notExist" {
		t.Fatalf("readlink: %s", err)
	}
}

func TestSyncFilesFromSingleFile(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	filesFrom := srcDir + "/files-from"
	if err := os.WriteFile(filesFrom, []byte("dir1/file1\ndir1/file2.txt\n"), 0644); err != nil {
		t.Fatalf("write files-from: %s", err)
	}

	src, _ := object.CreateStorage("file", srcDir+"/", "", "", "")
	if err := src.Put(ctx, "dir1/file1", bytes.NewReader([]byte("content1"))); err != nil {
		t.Fatalf("put file: %s", err)
	}
	if err := src.Put(ctx, "dir1/file2.txt", bytes.NewReader([]byte("content2"))); err != nil {
		t.Fatalf("put file: %s", err)
	}

	dst, _ := object.CreateStorage("file", dstDir+"/", "", "", "")
	if err := Sync(src, dst, &Config{
		Threads:     2,
		ListThreads: 1,
		Quiet:       true,
		FilesFrom:   filesFrom,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}

	if _, err := dst.Head(ctx, "dir1/file1"); err != nil {
		t.Fatalf("head dir1/file1: %s", err)
	}
	if _, err := dst.Head(ctx, "dir1/file2.txt"); err != nil {
		t.Fatalf("head dir1/file2.txt: %s", err)
	}
}

func TestSyncLinkWithOutFollow(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()

	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	a.Put(ctx, "a1", bytes.NewReader([]byte("test")))
	as := a.(object.SupportSymlink)
	as.Symlink("/tmp/a/a1", "l1")
	as.Symlink("./../notExist", "l3")

	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")

	if err := Sync(a, b, &Config{
		Threads:     50,
		ListThreads: 1,
		Update:      true,
		Perms:       true,
		Quiet:       true,
		ForceUpdate: true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}
	content, err := b.Get(ctx, "l1", 0, -1)
	if err != nil {
		t.Fatalf("get content error: %s", err)
	}
	if c, err := io.ReadAll(content); err != nil || string(c) != "test" {
		t.Fatalf("read content error: %s", err)
	}

	if lstat, err := os.Lstat("/tmp/b/l1"); err != nil && lstat.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("should follow link")
	}
	if _, err := os.Stat("/tmp/b/l3"); !os.IsNotExist(err) {
		t.Fatalf("should not copy broken link")
	}
}

func TestSingleLink(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()
	_ = os.Symlink("/tmp/aa", "/tmp/a")
	a, _ := object.CreateStorage("file", "/tmp/a", "", "", "")
	b, _ := object.CreateStorage("file", "/tmp/b", "", "", "")
	if err := Sync(a, b, &Config{
		Threads:     50,
		ListThreads: 1,
		Update:      true,
		Perms:       true,
		Links:       true,
		Quiet:       true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		ForceUpdate: true,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}
	readlink, _ := os.Readlink("/tmp/a")
	readlink2, err := os.Readlink("/tmp/b")
	if err != nil {
		t.Fatalf("sync err: %v", err)
	}

	if readlink != readlink2 || readlink != "/tmp/aa" {
		t.Fatalf("sync link failed")
	}
}

func TestSyncCheckAllLink(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()

	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	a.Put(ctx, "a1", bytes.NewReader([]byte("test")))
	as := a.(object.SupportSymlink)
	as.Symlink("/tmp/a/a1", "l1")

	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")
	bs := b.(object.SupportSymlink)
	bs.Symlink("/tmp/b/a1", "l1")

	if err := Sync(a, b, &Config{
		Threads:     50,
		Perms:       true,
		Links:       true,
		Quiet:       true,
		ListThreads: 1,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		CheckAll:    true,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}

	l1, err := bs.Readlink("l1")
	if err != nil || l1 != "/tmp/a/a1" {
		t.Fatalf("readlink: %s content: %s", err, l1)
	}
	content, err := b.Get(ctx, "l1", 0, -1)
	if err != nil {
		t.Fatalf("get content failed: %s", err)
	}
	if c, err := io.ReadAll(content); err != nil || string(c) != "test" {
		t.Fatalf("read content failed: err %s content %s", err, string(c))
	}
}

func TestSyncCheckNewLink(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a")
		_ = os.RemoveAll("/tmp/b")
	}()

	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	a.Put(ctx, "a1", bytes.NewReader([]byte("test")))
	as := a.(object.SupportSymlink)
	as.Symlink("/tmp/a/a1", "l1")

	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")
	bs := b.(object.SupportSymlink)

	if err := Sync(a, b, &Config{
		Threads:     50,
		Perms:       true,
		Links:       true,
		Quiet:       true,
		ListThreads: 1,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		CheckNew:    true,
	}); err != nil {
		t.Fatalf("sync: %s", err)
	}

	l1, err := bs.Readlink("l1")
	if err != nil || l1 != "/tmp/a/a1" {
		t.Fatalf("readlink: %s content: %s", err, l1)
	}
	content, err := b.Get(ctx, "l1", 0, -1)
	if err != nil {
		t.Fatalf("get content failed: %s", err)
	}
	if c, err := io.ReadAll(content); err != nil || string(c) != "test" {
		t.Fatalf("read content failed: err %s content %s", err, string(c))
	}
}

func TestLimits(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("/tmp/a/")
		_ = os.RemoveAll("/tmp/b/")
		_ = os.RemoveAll("/tmp/c/")
	}()
	a, _ := object.CreateStorage("file", "/tmp/a/", "", "", "")
	b, _ := object.CreateStorage("file", "/tmp/b/", "", "", "")
	c, _ := object.CreateStorage("file", "/tmp/c/", "", "", "")
	put := func(storage object.ObjectStorage, keys []string) {
		for _, key := range keys {
			if key != "" {
				_ = storage.Put(ctx, key, bytes.NewReader([]byte{}))
			}
		}
	}
	commonKeys := []string{"", "a1", "a2", "a3", "a4", "a5", "a6"}
	put(a, commonKeys)
	put(c, []string{"c1", "c2", "c3"})
	type subConfig struct {
		dst          object.ObjectStorage
		limit        int64
		deleteDst    bool
		expectedKeys []string
	}
	testCases := []subConfig{
		{b, 2, false, []string{"", "a1", "a2"}},
		{b, -1, false, commonKeys},
		{b, 0, false, commonKeys},
		{c, 7, true, append(commonKeys, "c2", "c3")},
	}
	config := &Config{
		Threads:     50,
		Update:      true,
		Perms:       true,
		MaxSize:     math.MaxInt64,
		ListThreads: 1,
	}
	setConfig := func(config *Config, subC subConfig) {
		config.Limit = subC.limit
		config.DeleteDst = subC.deleteDst
	}

	for _, tcase := range testCases {
		setConfig(config, tcase)
		if err := Sync(a, tcase.dst, config); err != nil {
			t.Fatalf("sync: %s", err)
		}

		all, err := ListAll(tcase.dst, "", "", "", true)
		if err != nil {
			t.Fatalf("list all b: %s", err)
		}

		err = testKeysEqual(all, tcase.expectedKeys)
		if err != nil {
			t.Fatalf("testKeysEqual fail: %s", err)
		}
	}
}

func testKeysEqual(objsCh <-chan object.Object, expectedKeys []string) error {
	var gottenKeys []string
	for obj := range objsCh {
		gottenKeys = append(gottenKeys, obj.Key())
	}
	if len(gottenKeys) != len(expectedKeys) {
		return fmt.Errorf("expected {%s}, got {%s}", strings.Join(expectedKeys, ", "),
			strings.Join(gottenKeys, ", "))
	}

	for idx, key := range gottenKeys {
		if key != expectedKeys[idx] {
			return fmt.Errorf("expected {%s}, got {%s}", strings.Join(expectedKeys, ", "),
				strings.Join(gottenKeys, ", "))
		}
	}
	return nil
}

func TestMatchObjects(t *testing.T) {
	type tcase struct {
		rules []rule
		key   string
		want  bool
	}
	tests := []tcase{
		{rules: []rule{{pattern: "a*"}}, key: "a1"},
		{rules: []rule{{pattern: "a*/b*"}}, key: "a1/b1"},
		{rules: []rule{{pattern: "/a*"}}, key: "/a1"},
		{rules: []rule{{pattern: "/a"}}, key: "/a1", want: true},
		{rules: []rule{{pattern: "/a/b/c"}}, key: "/a1", want: true},
		{rules: []rule{{pattern: "a*/b?"}}, key: "a1/b1/c2/d1"},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/", want: true},
		{rules: []rule{{pattern: "a*/b?/c.txt"}}, key: "a1/b1", want: true},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/b1/"},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/b1/c.txt"},
		{rules: []rule{{pattern: "a*/"}}, key: "a1/b1"},
		{rules: []rule{{pattern: "a*/b*/"}}, key: "a1/b1/c1/d.txt/"},
		{rules: []rule{{pattern: "/a*/b*"}}, key: "/a1/b1/c1/d.txt/"},
		{rules: []rule{{pattern: "a*/b*/c"}}, key: "a1/b1/c1/d.txt/", want: true},
		{rules: []rule{{pattern: "a"}}, key: "a/b/c/d/"},
		{rules: []rule{{pattern: "a.go", include: true}, {pattern: "pkg"}}, key: "a/pkg/c/a.go"},
		{rules: []rule{{pattern: "a"}, {pattern: "pkg", include: true}}, key: "a/pkg/c/a.go"},
		{rules: []rule{{pattern: "a.go", include: true}, {pattern: "pkg"}}, key: "", want: true},
		{rules: []rule{{pattern: "a", include: true}, {pattern: "b/"}, {pattern: "c", include: true}}, key: "a/b/c"},
		{rules: []rule{{pattern: "a/", include: true}, {pattern: "a"}}, key: "a/b", want: true},
		{rules: []rule{{pattern: "/***"}}, key: "a"},
		{rules: []rule{{pattern: "/***"}}, key: "a/b"},
		{rules: []rule{{pattern: "/a/***"}}, key: "a/"},
		{rules: []rule{{pattern: "/a/***"}}, key: "a/b"},
		{rules: []rule{{pattern: "/a/***"}}, key: "a/b/c"},
		{rules: []rule{{pattern: "/a/***"}}, key: "b/a/", want: true},
		{rules: []rule{{pattern: "a/***"}}, key: "a/"},
		{rules: []rule{{pattern: "a/***"}}, key: "a/b"},
		{rules: []rule{{pattern: "a/***"}}, key: "a/b/c"},
		{rules: []rule{{pattern: "a/***"}}, key: "d/a/b/c"},
		{rules: []rule{{pattern: "a/***"}}, key: "a", want: true},
		{rules: []rule{{pattern: "a/***"}}, key: "ba", want: true},
		{rules: []rule{{pattern: "a/***"}}, key: "ba/", want: true},
		{rules: []rule{{pattern: "*/a/***"}}, key: "/a/"},
		{rules: []rule{{pattern: "*/a/***"}}, key: "b/a/"},
		{rules: []rule{{pattern: "*/a/***"}}, key: "b/a/c"},
		{rules: []rule{{pattern: "/*/a/***"}}, key: "/b/a/"},
		{rules: []rule{{pattern: "/*/a/***"}}, key: "/b/a/c"},
		{rules: []rule{{pattern: "/*/a/***"}}, key: "c/b/a/", want: true},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/b"},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/d/b"},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/d/e/b"},
		{rules: []rule{{pattern: "/**/b"}}, key: "a/c/b"},
		{rules: []rule{{pattern: "/**/b"}}, key: "a/c/d/b/"},
		{rules: []rule{{pattern: "a**/b"}}, key: "a/c/d/b/"},
		{rules: []rule{{pattern: "a**/b"}}, key: "a/c/d/ab/", want: true},
		{rules: []rule{{pattern: "a**b"}}, key: "a/c/d/b/"},
		{rules: []rule{{pattern: "a**b"}}, key: "b/c/d/b/", want: true},
		{rules: []rule{{pattern: "a?**"}}, key: "a/a", want: true},
		{rules: []rule{{pattern: "**a"}}, key: "a"},
		{rules: []rule{{pattern: "a**"}}, key: "a"},
		{rules: []rule{{pattern: "a**a"}}, key: "a", want: true},
		{rules: []rule{{pattern: "aa**a"}}, key: "aa", want: true},
		{rules: []rule{{pattern: "**/d2/**a"}}, key: "/d2/d3/1a"},
		{rules: []rule{{pattern: "**/d2/**a"}}, key: "d2/d3/1a"},
		{rules: []rule{{pattern: "a/**/a"}}, key: "a", want: true},
		{rules: []rule{{pattern: "a/**/a"}}, key: "a/", want: true},
		{rules: []rule{{pattern: "**aa**", include: true}, {pattern: "a"}}, key: "aa/a", want: true},
	}
	for _, c := range tests {
		if got := matchLeveledPath(c.rules, c.key); got != c.want {
			t.Errorf("matchKey(%+v, %s) = %v, want %v", c.rules, c.key, got, c.want)
		}
	}
}

func TestMatchFullPatch(t *testing.T) {
	type tcase struct {
		rules []rule
		key   string
	}
	matchedCases := []tcase{
		{rules: []rule{{pattern: "a"}}, key: "b/a"},
		{rules: []rule{{pattern: "a*"}}, key: "a1"},
		{rules: []rule{{pattern: "a*/b*"}}, key: "a1/b1"},
		{rules: []rule{{pattern: "/a*"}}, key: "/a1"},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/b1/"},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/b"},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/d/b"},
		{rules: []rule{{pattern: "a/**/b"}}, key: "a/c/d/e/b"},
		{rules: []rule{{pattern: "/**/b"}}, key: "a/c/b"},
		{rules: []rule{{pattern: "a**/b"}}, key: "a/c/d/b"},
		{rules: []rule{{pattern: "a**b"}}, key: "a/c/d/b"},
		{rules: []rule{{pattern: "**a"}}, key: "a"},
		{rules: []rule{{pattern: "a**"}}, key: "a"},
		{rules: []rule{{pattern: "**/d2/**a"}}, key: "/d2/d3/1a"},
		{rules: []rule{{pattern: "**/d2/**a"}}, key: "d2/d3/1a"},
	}
	for _, c := range matchedCases {
		if got := matchFullPath(c.rules, c.key); got != false {
			t.Errorf("matchKey(%+v, %s) = %v, want %v", c.rules, c.key, got, false)
		}
	}
	unmatchedCases := []tcase{
		{rules: []rule{{pattern: "/a"}}, key: "/a1"},
		{rules: []rule{{pattern: "a*/b?"}}, key: "a1/b1/c2/d1"},
		{rules: []rule{{pattern: "/a/b/c"}}, key: "/a1"},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/"},
		{rules: []rule{{pattern: "a*/b?/c.txt"}}, key: "a1/b1"},
		{rules: []rule{{pattern: "a*/b?/"}}, key: "a1/b1/c.txt"},
		{rules: []rule{{pattern: "a*/"}}, key: "a1/b1"},
		{rules: []rule{{pattern: "a*/b*/"}}, key: "a1/b1/c1/d.txt/"},
		{rules: []rule{{pattern: "/a*/b*"}}, key: "/a1/b1/c1/d.txt/"},
		{rules: []rule{{pattern: "a"}}, key: "a/b/c/d/"},
		{rules: []rule{{pattern: "a*/b*/c"}}, key: "a1/b1/c1/d.txt/"},
		{rules: []rule{{pattern: "a**/b"}}, key: "a/c/d/ab/"},
		{rules: []rule{{pattern: "a**b"}}, key: "b/c/d/b"},
		{rules: []rule{{pattern: "/**/b"}}, key: "a/c/d/b/"},
		{rules: []rule{{pattern: "a?**"}}, key: "a/a"},
		{rules: []rule{{pattern: "a**a"}}, key: "a"},
		{rules: []rule{{pattern: "aa**a"}}, key: "aa"},
		{rules: []rule{{pattern: "a/**/a"}}, key: "a"},
		{rules: []rule{{pattern: "a/**/a"}}, key: "a/"},
		{rules: []rule{{pattern: "**aa**", include: true}, {pattern: "a"}}, key: "aa/a"},
	}
	for _, c := range unmatchedCases {
		if got := matchFullPath(c.rules, c.key); got != true {
			t.Errorf("matchKey(%+v, %s) = %v, want %v", c.rules, c.key, got, true)
		}
	}
}

func TestParseFilterRule(t *testing.T) {
	type tcase struct {
		args  []string
		rules []rule
	}
	cases := []tcase{
		{[]string{"--include", "a"}, []rule{{pattern: "a", include: true}}},
		{[]string{"--exclude", "a", "--include", "b"}, []rule{{pattern: "a"}, {pattern: "b", include: true}}},
		{[]string{"--include", "a", "--test", "t", "--exclude", "b"}, []rule{{pattern: "a", include: true}, {pattern: "b"}}},
		{[]string{"--include=a", "--test", "t", "--exclude"}, []rule{{pattern: "a", include: true}}},
		{[]string{"--include", "a", "--test", "t", "--exclude"}, []rule{{pattern: "a", include: true}}},
		{[]string{"-include=", "a", "--test", "t", "--exclude=*"}, []rule{{pattern: "*"}}},
	}

	for _, c := range cases {
		if got := parseIncludeRules(c.args); !reflect.DeepEqual(got, c.rules) {
			t.Errorf("parseIncludeRules(%+v) = %v, want %v", c.args, got, c.rules)
		}
	}
}

func TestFilterSizeAndAge(t *testing.T) {
	config := &Config{
		MaxSize: 100,
		MinSize: 10,
		MaxAge:  time.Second * 100,
		MinAge:  time.Second * 10,
	}
	now := time.Now()
	if !filterKey(&mockObject{size: 10, mtime: now.Add(-time.Second * 15)}, now, nil, config) {
		t.Fatalf("filterKey failed")
	}
	if filterKey(&mockObject{size: 200, mtime: now.Add(-time.Second * 200)}, now, nil, config) {
		t.Fatalf("filterKey should fail")
	}

	config = &Config{
		MaxSize:   math.MaxInt64,
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now().Add(-time.Minute),
	}
	if !filterKey(&mockObject{size: 200, mtime: now.Add(-time.Minute * 30)}, now, nil, config) {
		t.Fatalf("filterKey fail")
	}

	if filterKey(&mockObject{size: 200, mtime: now.Add(-time.Hour * 2)}, now, nil, config) {
		t.Fatalf("filterKey should fail")
	}
}

// nolint:errcheck
func TestSyncEncrypt(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %s", err)
	}
	kc := object.NewRSAEncryptor(rsaKey)
	enc, err := object.NewDataEncryptor(kc, object.AES256GCM_RSA)
	if err != nil {
		t.Fatalf("create encryptor: %s", err)
	}

	// sync plaintext src -> encrypted dst
	src, _ := object.CreateStorage("mem", "", "", "", "")
	dst, _ := object.CreateStorage("mem", "", "", "", "")

	testData := map[string]string{
		"file1.txt":     "hello world",
		"dir/file2.txt": "foo bar baz",
		"empty.txt":     "x",
	}
	for k, v := range testData {
		src.Put(ctx, k, bytes.NewReader([]byte(v)))
	}

	encDst := object.NewChunkedEncrypted(dst, enc)
	if err := Sync(src, encDst, &Config{
		Threads:     10,
		ListThreads: 1,
		Update:      true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		Quiet:       true,
	}); err != nil {
		t.Fatalf("sync to encrypted dst: %s", err)
	}

	// Verify dst has encrypted data (raw read should differ from plaintext)
	for k, v := range testData {
		r, err := dst.Get(ctx, k, 0, -1)
		if err != nil {
			t.Fatalf("get raw %s: %s", k, err)
		}
		raw, _ := io.ReadAll(r)
		if string(raw) == v {
			t.Fatalf("data for %s should be encrypted but got plaintext", k)
		}
	}

	// sync encrypted src -> plaintext dst (decrypt)
	dst2, _ := object.CreateStorage("mem", "", "", "", "")
	encSrc := object.NewChunkedEncrypted(dst, enc)
	if err := Sync(encSrc, dst2, &Config{
		Threads:     10,
		ListThreads: 1,
		Update:      true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		Quiet:       true,
	}); err != nil {
		t.Fatalf("sync from encrypted src: %s", err)
	}

	// Verify dst2 has original plaintext
	for k, v := range testData {
		r, err := dst2.Get(ctx, k, 0, -1)
		if err != nil {
			t.Fatalf("get decrypted %s: %s", k, err)
		}
		data, _ := io.ReadAll(r)
		if string(data) != v {
			t.Fatalf("decrypted %s: got %q, want %q", k, string(data), v)
		}
	}

	// decrypt with wrong key should fail
	rsaKey2, _ := rsa.GenerateKey(rand.Reader, 2048)
	kc2 := object.NewRSAEncryptor(rsaKey2)
	enc2, _ := object.NewDataEncryptor(kc2, object.AES256GCM_RSA)
	wrongSrc := object.NewChunkedEncrypted(dst, enc2)
	dst3, _ := object.CreateStorage("mem", "", "", "", "")
	err = Sync(wrongSrc, dst3, &Config{
		Threads:     10,
		ListThreads: 1,
		Update:      true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		Quiet:       true,
	})
	// Sync should complete but with failures (wrong key can't decrypt)
	ch, _ := ListAll(dst3, "", "", "", true)
	var count int
	for range ch {
		count++
	}
	if count == len(testData) {
		t.Fatalf("decrypting with wrong key should not produce all files")
	}
}

// nolint:errcheck
func TestSyncEncryptLargeFile(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %s", err)
	}
	kc := object.NewRSAEncryptor(rsaKey)
	enc, err := object.NewDataEncryptor(kc, object.AES256GCM_RSA)
	if err != nil {
		t.Fatalf("create encryptor: %s", err)
	}

	src, _ := object.CreateStorage("mem", "", "", "", "")
	dst, _ := object.CreateStorage("mem", "", "", "", "")

	// Create a large file that spans multiple chunks (>8 MiB)
	largeData := make([]byte, 9<<20) // 9 MiB
	for i := range largeData {
		largeData[i] = byte(i % 253)
	}
	src.Put(ctx, "large.bin", bytes.NewReader(largeData))

	encDst := object.NewChunkedEncrypted(dst, enc)
	if err := Sync(src, encDst, &Config{
		Threads:     10,
		ListThreads: 1,
		Update:      true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		Quiet:       true,
	}); err != nil {
		t.Fatalf("sync large file to encrypted dst: %s", err)
	}

	// Verify encrypted data differs from plaintext
	r, err := dst.Get(ctx, "large.bin", 0, -1)
	if err != nil {
		t.Fatalf("get raw large.bin: %s", err)
	}
	raw, _ := io.ReadAll(r)
	if bytes.Equal(raw, largeData) {
		t.Fatalf("large file should be encrypted")
	}

	// Decrypt back
	dst2, _ := object.CreateStorage("mem", "", "", "", "")
	encSrc := object.NewChunkedEncrypted(dst, enc)
	if err := Sync(encSrc, dst2, &Config{
		Threads:     10,
		ListThreads: 1,
		Update:      true,
		Limit:       -1,
		MaxSize:     math.MaxInt64,
		Quiet:       true,
	}); err != nil {
		t.Fatalf("sync large file from encrypted src: %s", err)
	}

	r, err = dst2.Get(ctx, "large.bin", 0, -1)
	if err != nil {
		t.Fatalf("get decrypted large.bin: %s", err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, largeData) {
		t.Fatalf("decrypted large file mismatch: got %d bytes, want %d", len(got), len(largeData))
	}
}

// mockDbServiceForSync 用于验证 recordSyncObject 的白名单过滤。
type mockDbServiceForSync struct {
	mu      stdsync.Mutex
	records []sync_db.ObjectRecord
}

func (m *mockDbServiceForSync) StartJob(job sync_db.JobInfo) error { return nil }
func (m *mockDbServiceForSync) RecordObject(rec sync_db.ObjectRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, rec)
	return nil
}
func (m *mockDbServiceForSync) RecordObjects(recs []sync_db.ObjectRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, recs...)
	return nil
}
func (m *mockDbServiceForSync) EndJob(jobID string, job sync_db.JobInfo) error { return nil }
func (m *mockDbServiceForSync) Close() error                                   { return nil }

func (m *mockDbServiceForSync) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

func (m *mockDbServiceForSync) statuses() []sync_db.ObjectStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sync_db.ObjectStatus, len(m.records))
	for i, r := range m.records {
		out[i] = r.Status
	}
	return out
}

func TestParseDbRecordStatus(t *testing.T) {
	// 空值：不过滤
	set, err := parseDbRecordStatus(nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if set != nil {
		t.Fatalf("expected nil set for empty input, got %v", set)
	}

	// 普通同步：合法值
	set, err = parseDbRecordStatus([]string{"copied", "failed"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(set) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(set))
	}
	for _, s := range []sync_db.ObjectStatus{sync_db.StatusCopied, sync_db.StatusFailed} {
		if _, ok := set[s]; !ok {
			t.Fatalf("expected status %v in set", s)
		}
	}

	// 大小写、空格、去重
	set, err = parseDbRecordStatus([]string{"  Copied ", "FAILED", "copied"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(set) != 2 {
		t.Fatalf("expected 2 unique statuses, got %d", len(set))
	}

	// 非法值
	_, err = parseDbRecordStatus([]string{"copied", "unknown"}, false)
	if err == nil {
		t.Fatalf("expected error for invalid status")
	}

	// scan 模式包含 scan 特有状态
	set, err = parseDbRecordStatus([]string{"copied", "failed"}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(set) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(set))
	}
	_, ok := set[sync_db.StatusMissing]
	if ok {
		t.Fatalf("did not expect missing status with explicit copied,failed")
	}
}

func TestRecordSyncObjectStatusFilter(t *testing.T) {
	mock := &mockDbServiceForSync{}
	syncDbService = sync_db.NewAsyncDbService(mock)
	defer syncDbService.Close()

	// 默认白名单 copied/failed，skipped 不应写入
	set, err := parseDbRecordStatus([]string{"copied", "failed"}, false)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	oldSet := dbRecordStatusSet
	dbRecordStatusSet = set
	defer func() { dbRecordStatusSet = oldSet }()

	recordSyncObject("job1", "a", 1, time.Now(), sync_db.StatusCopied, "")
	recordSyncObject("job1", "b", 1, time.Now(), sync_db.StatusFailed, "err")
	recordSyncObject("job1", "c", 1, time.Now(), sync_db.StatusSkipped, "")
	recordSyncObject("job1", "d", 1, time.Now(), sync_db.StatusDeleted, "")

	if err := syncDbService.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if mock.count() != 2 {
		t.Fatalf("expected 2 recorded objects, got %d", mock.count())
	}
	statuses := mock.statuses()
	for _, s := range statuses {
		if s != sync_db.StatusCopied && s != sync_db.StatusFailed {
			t.Fatalf("unexpected status %v recorded", s)
		}
	}
}

// TestScanSingleFullKey 验证 --full-key：CSV 记录完整 key（含 URL 前缀），
// 默认记录相对前缀的 key。
func TestScanSingleFullKey(t *testing.T) {
	store, _ := object.CreateStorage("mem", "", "", "", "")
	for _, k := range []string{"[a_attax", "[a_atta/y/z", "other"} {
		if err := store.Put(ctx, k, bytes.NewReader([]byte("d"))); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	wrapped := object.WithPrefix(store, "[a_atta")

	run := func(fullKey bool) []string {
		f, err := os.CreateTemp("", "scan*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())

		outputCSVFile = f
		outputCSV = csv.NewWriter(f)
		oldService := syncDbService
		syncDbService = nil
		defer func() {
			syncDbService = oldService
			outputCSV = nil
			outputCSVFile = nil
		}()

		if err := scanSingle(wrapped, &Config{FullKey: fullKey}); err != nil {
			t.Fatalf("scanSingle: %v", err)
		}
		outputCSV.Flush()
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n")[1:] {
			keys = append(keys, strings.Split(line, ",")[0])
		}
		return keys
	}

	full := run(true)
	if len(full) != 2 {
		t.Fatalf("expected 2 scanned keys, got %v", full)
	}
	for _, k := range full {
		if !strings.HasPrefix(k, "[a_atta") {
			t.Fatalf("full-key mode: expected key with prefix, got %q", k)
		}
	}

	rel := run(false)
	if len(rel) != 2 {
		t.Fatalf("expected 2 scanned keys, got %v", rel)
	}
	for _, k := range rel {
		if strings.HasPrefix(k, "[a_atta") {
			t.Fatalf("default mode: expected relative key, got %q", k)
		}
	}
}
