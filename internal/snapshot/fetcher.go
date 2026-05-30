package snapshot

import (
	"context"
	"fmt"
	"io"
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

type Fetcher interface {
	FetchAll(context.Context, []RemoteFile) (string, error)
}

type SchemeFetcher interface {
	Fetch(context.Context, RemoteFile, string) error
}

type MultiFetcher struct {
	schemes map[string]SchemeFetcher
}

func NewMultiFetcher(schemes map[string]SchemeFetcher) *MultiFetcher {
	out := make(map[string]SchemeFetcher, len(schemes))
	for name, fetcher := range schemes {
		out[strings.ToLower(name)] = fetcher
	}
	return &MultiFetcher{schemes: out}
}

func (f *MultiFetcher) FetchAll(ctx context.Context, files []RemoteFile) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("snapshot: at least one snapshot file is required")
	}
	localDir, err := os.MkdirTemp("", "fluss-kv-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("snapshot: create temp dir: %w", err)
	}
	for _, file := range files {
		u, err := url.Parse(file.RemotePath)
		if err != nil {
			_ = os.RemoveAll(localDir)
			return "", fmt.Errorf("snapshot: parse remote path %q: %w", file.RemotePath, err)
		}
		fetcher, ok := f.schemes[strings.ToLower(u.Scheme)]
		if !ok {
			_ = os.RemoveAll(localDir)
			return "", fmt.Errorf("snapshot: unsupported remote path scheme %q", u.Scheme)
		}
		if err := fetcher.Fetch(ctx, file, localDir); err != nil {
			_ = os.RemoveAll(localDir)
			return "", err
		}
	}
	return localDir, nil
}

func NewDefaultFetcher(cfg StorageConfig) (Fetcher, error) {
	schemes := map[string]SchemeFetcher{
		"file": fileFetcher{},
		"":     fileFetcher{},
	}
	if cfg.S3Endpoint != "" || cfg.S3AccessKey != "" || cfg.S3SecretKey != "" || cfg.S3BucketOverride != "" {
		s3Fetcher, err := NewS3Fetcher(cfg)
		if err != nil {
			return nil, err
		}
		schemes["s3"] = s3Fetcher
	}
	return NewMultiFetcher(schemes), nil
}

type S3Fetcher struct {
	cfg StorageConfig
}

func NewS3Fetcher(cfg StorageConfig) (*S3Fetcher, error) {
	if cfg.S3Endpoint == "" {
		return nil, fmt.Errorf("snapshot: s3 endpoint is required")
	}
	if cfg.S3AccessKey == "" || cfg.S3SecretKey == "" {
		return nil, fmt.Errorf("snapshot: s3 access key and secret key are required")
	}
	if cfg.S3Region == "" {
		cfg.S3Region = "us-east-1"
	}
	return &S3Fetcher{cfg: cfg}, nil
}

func (f *S3Fetcher) Fetch(ctx context.Context, file RemoteFile, localDir string) error {
	mc, err := minio.New(f.cfg.S3Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(f.cfg.S3AccessKey, f.cfg.S3SecretKey, ""),
		Secure:       f.cfg.S3UseSSL,
		Region:       f.cfg.S3Region,
		BucketLookup: bucketLookup(f.cfg.S3PathStyle),
	})
	if err != nil {
		return fmt.Errorf("snapshot: create minio client: %w", err)
	}
	bucket, objectKey, err := f.remoteObject(file.RemotePath)
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
	return nil
}

func (f *S3Fetcher) remoteObject(remotePath string) (bucket string, objectKey string, err error) {
	u, err := url.Parse(remotePath)
	if err != nil {
		return "", "", fmt.Errorf("snapshot: parse remote path %q: %w", remotePath, err)
	}
	bucket = u.Host
	if f.cfg.S3BucketOverride != "" {
		bucket = f.cfg.S3BucketOverride
	}
	objectKey = strings.TrimPrefix(u.Path, "/")
	if bucket == "" || objectKey == "" {
		return "", "", fmt.Errorf("snapshot: invalid remote path %q", remotePath)
	}
	return bucket, objectKey, nil
}

type fileFetcher struct{}

func (fileFetcher) Fetch(ctx context.Context, file RemoteFile, localDir string) error {
	_ = ctx
	u, err := url.Parse(file.RemotePath)
	if err != nil {
		return fmt.Errorf("snapshot: parse remote path %q: %w", file.RemotePath, err)
	}
	sourcePath := u.Path
	if u.Scheme == "" {
		sourcePath = file.RemotePath
	}
	if sourcePath == "" {
		return fmt.Errorf("snapshot: invalid remote path %q", file.RemotePath)
	}
	localPath := filepath.Join(localDir, file.LocalFileName)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("snapshot: create local file dir: %w", err)
	}
	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("snapshot: open local source %s: %w", file.RemotePath, err)
	}
	defer func() { _ = src.Close() }()
	dst, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("snapshot: create local destination: %w", err)
	}
	defer func() { _ = dst.Close() }()
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("snapshot: copy local source %s: %w", file.RemotePath, err)
	}
	return nil
}

func bucketLookup(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}
