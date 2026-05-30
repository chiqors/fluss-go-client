package snapshot

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type StorageConfig struct {
	S3Endpoint       string
	S3AccessKey      string
	S3SecretKey      string
	S3Region         string
	S3UseSSL         bool
	S3PathStyle      bool
	S3BucketOverride string
}

type RemoteFile struct {
	RemotePath    string
	LocalFileName string
}

type Downloader struct {
	cfg StorageConfig
}

func NewDownloader(cfg StorageConfig) (*Downloader, error) {
	if cfg.S3Endpoint == "" {
		return nil, fmt.Errorf("snapshot: s3 endpoint is required")
	}
	if cfg.S3AccessKey == "" || cfg.S3SecretKey == "" {
		return nil, fmt.Errorf("snapshot: s3 access key and secret key are required")
	}
	if cfg.S3Region == "" {
		cfg.S3Region = "us-east-1"
	}
	return &Downloader{cfg: cfg}, nil
}

func (d *Downloader) DownloadAll(ctx context.Context, files []RemoteFile) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("snapshot: at least one snapshot file is required")
	}
	localDir, err := os.MkdirTemp("", "fluss-kv-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("snapshot: create temp dir: %w", err)
	}
	if err := d.downloadAll(ctx, localDir, files); err != nil {
		_ = os.RemoveAll(localDir)
		return "", err
	}
	return localDir, nil
}

func (d *Downloader) downloadAll(ctx context.Context, localDir string, files []RemoteFile) error {
	mc, err := minio.New(d.cfg.S3Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(d.cfg.S3AccessKey, d.cfg.S3SecretKey, ""),
		Secure:       d.cfg.S3UseSSL,
		Region:       d.cfg.S3Region,
		BucketLookup: bucketLookup(d.cfg.S3PathStyle),
	})
	if err != nil {
		return fmt.Errorf("snapshot: create minio client: %w", err)
	}
	for _, file := range files {
		bucket, objectKey, err := d.remoteObject(file.RemotePath)
		if err != nil {
			return err
		}
		localPath := filepath.Join(localDir, file.LocalFileName)
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return fmt.Errorf("snapshot: create local file dir: %w", err)
		}
		if err := mc.FGetObject(ctx, bucket, objectKey, localPath, minio.GetObjectOptions{}); err != nil {
			return fmt.Errorf("snapshot: download %s: %w", file.RemotePath, err)
		}
	}
	return nil
}

func (d *Downloader) remoteObject(remotePath string) (bucket string, objectKey string, err error) {
	u, err := url.Parse(remotePath)
	if err != nil {
		return "", "", fmt.Errorf("snapshot: parse remote path %q: %w", remotePath, err)
	}
	if !strings.EqualFold(u.Scheme, "s3") {
		return "", "", fmt.Errorf("snapshot: unsupported remote path scheme %q", u.Scheme)
	}
	bucket = u.Host
	if d.cfg.S3BucketOverride != "" {
		bucket = d.cfg.S3BucketOverride
	}
	objectKey = strings.TrimPrefix(u.Path, "/")
	if bucket == "" || objectKey == "" {
		return "", "", fmt.Errorf("snapshot: invalid remote path %q", remotePath)
	}
	return bucket, objectKey, nil
}

func bucketLookup(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}
