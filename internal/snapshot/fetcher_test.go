package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestS3FetcherRemoteObject(t *testing.T) {
	fetcher, err := NewS3Fetcher(StorageConfig{
		S3Endpoint:  "127.0.0.1:9000",
		S3AccessKey: "a",
		S3SecretKey: "b",
	})
	if err != nil {
		t.Fatalf("NewS3Fetcher() error = %v", err)
	}
	bucket, key, err := fetcher.remoteObject("s3://fluss/remote-data/kv/0001.sst")
	if err != nil {
		t.Fatalf("remoteObject() error = %v", err)
	}
	if bucket != "fluss" || key != "remote-data/kv/0001.sst" {
		t.Fatalf("remoteObject() = (%q, %q), want (%q, %q)", bucket, key, "fluss", "remote-data/kv/0001.sst")
	}
}

func TestS3FetcherRemoteObjectBucketOverride(t *testing.T) {
	fetcher, err := NewS3Fetcher(StorageConfig{
		S3Endpoint:       "127.0.0.1:9000",
		S3AccessKey:      "a",
		S3SecretKey:      "b",
		S3BucketOverride: "override",
	})
	if err != nil {
		t.Fatalf("NewS3Fetcher() error = %v", err)
	}
	bucket, key, err := fetcher.remoteObject("s3://fluss/remote-data/kv/0001.sst")
	if err != nil {
		t.Fatalf("remoteObject() error = %v", err)
	}
	if bucket != "override" || key != "remote-data/kv/0001.sst" {
		t.Fatalf("remoteObject() = (%q, %q), want (%q, %q)", bucket, key, "override", "remote-data/kv/0001.sst")
	}
}

func TestMultiFetcherLocalFileScheme(t *testing.T) {
	sourceDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "0001.sst")
	if err := os.WriteFile(sourceFile, []byte("snapshot-bytes"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	fetcher := NewMultiFetcher(map[string]SchemeFetcher{
		"file": fileFetcher{},
		"":     fileFetcher{},
	})
	localDir, err := fetcher.FetchAll(context.Background(), []RemoteFile{{
		RemotePath:    "file://" + sourceFile,
		LocalFileName: "nested/0001.sst",
	}})
	if err != nil {
		t.Fatalf("FetchAll() error = %v", err)
	}
	defer func() { _ = os.RemoveAll(localDir) }()

	got, err := os.ReadFile(filepath.Join(localDir, "nested/0001.sst"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "snapshot-bytes" {
		t.Fatalf("fetched bytes = %q, want %q", string(got), "snapshot-bytes")
	}
}

func TestMultiFetcherPlainLocalPath(t *testing.T) {
	sourceDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "MANIFEST")
	if err := os.WriteFile(sourceFile, []byte("manifest"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	fetcher := NewMultiFetcher(map[string]SchemeFetcher{
		"": fileFetcher{},
	})
	localDir, err := fetcher.FetchAll(context.Background(), []RemoteFile{{
		RemotePath:    sourceFile,
		LocalFileName: "MANIFEST",
	}})
	if err != nil {
		t.Fatalf("FetchAll() error = %v", err)
	}
	defer func() { _ = os.RemoveAll(localDir) }()

	got, err := os.ReadFile(filepath.Join(localDir, "MANIFEST"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "manifest" {
		t.Fatalf("fetched bytes = %q, want %q", string(got), "manifest")
	}
}

func TestNewDefaultFetcherWithoutS3ConfigSupportsLocalFiles(t *testing.T) {
	sourceDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "CURRENT")
	if err := os.WriteFile(sourceFile, []byte("current"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	fetcher, err := NewDefaultFetcher(StorageConfig{})
	if err != nil {
		t.Fatalf("NewDefaultFetcher() error = %v", err)
	}
	localDir, err := fetcher.FetchAll(context.Background(), []RemoteFile{{
		RemotePath:    sourceFile,
		LocalFileName: "CURRENT",
	}})
	if err != nil {
		t.Fatalf("FetchAll() error = %v", err)
	}
	defer func() { _ = os.RemoveAll(localDir) }()

	got, err := os.ReadFile(filepath.Join(localDir, "CURRENT"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "current" {
		t.Fatalf("fetched bytes = %q, want %q", string(got), "current")
	}
}
