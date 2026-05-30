package client

import (
	"context"
	"fmt"

	"github.com/chiqors/fluss-go-client/internal/snapshot"
)

type SnapshotFetcher interface {
	FetchAll(context.Context, []snapshot.RemoteFile) (string, error)
}

func (c *Client) SnapshotFetcher() (SnapshotFetcher, error) {
	if c.cfg.SnapshotFetcher != nil {
		return c.cfg.SnapshotFetcher, nil
	}
	fetcher, err := snapshot.NewDefaultFetcher(snapshot.StorageConfig{
		S3Endpoint:       c.cfg.SnapshotStorage.S3Endpoint,
		S3AccessKey:      c.cfg.SnapshotStorage.S3AccessKey,
		S3SecretKey:      c.cfg.SnapshotStorage.S3SecretKey,
		S3Region:         c.cfg.SnapshotStorage.S3Region,
		S3UseSSL:         c.cfg.SnapshotStorage.S3UseSSL,
		S3PathStyle:      c.cfg.SnapshotStorage.S3PathStyle,
		S3BucketOverride: c.cfg.SnapshotStorage.S3BucketOverride,
	})
	if err != nil {
		return nil, fmt.Errorf("fluss: build snapshot fetcher: %w", err)
	}
	return fetcher, nil
}
