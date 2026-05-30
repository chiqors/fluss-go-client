package snapshot

import "testing"

func TestDownloaderRemoteObject(t *testing.T) {
	d, err := NewDownloader(StorageConfig{
		S3Endpoint:  "127.0.0.1:9000",
		S3AccessKey: "a",
		S3SecretKey: "b",
	})
	if err != nil {
		t.Fatalf("NewDownloader() error = %v", err)
	}
	bucket, key, err := d.remoteObject("s3://fluss/remote-data/kv/0001.sst")
	if err != nil {
		t.Fatalf("remoteObject() error = %v", err)
	}
	if bucket != "fluss" || key != "remote-data/kv/0001.sst" {
		t.Fatalf("remoteObject() = (%q, %q), want (%q, %q)", bucket, key, "fluss", "remote-data/kv/0001.sst")
	}
}

func TestDownloaderRemoteObjectBucketOverride(t *testing.T) {
	d, err := NewDownloader(StorageConfig{
		S3Endpoint:       "127.0.0.1:9000",
		S3AccessKey:      "a",
		S3SecretKey:      "b",
		S3BucketOverride: "override",
	})
	if err != nil {
		t.Fatalf("NewDownloader() error = %v", err)
	}
	bucket, key, err := d.remoteObject("s3://fluss/remote-data/kv/0001.sst")
	if err != nil {
		t.Fatalf("remoteObject() error = %v", err)
	}
	if bucket != "override" || key != "remote-data/kv/0001.sst" {
		t.Fatalf("remoteObject() = (%q, %q), want (%q, %q)", bucket, key, "override", "remote-data/kv/0001.sst")
	}
}
