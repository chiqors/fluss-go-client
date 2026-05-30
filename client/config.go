package client

import (
	"net"
	"time"

	"github.com/chiqors/fluss-go-client/internal/auth"
	"github.com/chiqors/fluss-go-client/internal/snapshot"
)

type SnapshotStorageConfig struct {
	S3Endpoint       string
	S3AccessKey      string
	S3SecretKey      string
	S3Region         string
	S3UseSSL         bool
	S3PathStyle      bool
	S3BucketOverride string
}

type Config struct {
	Endpoints             []string
	ClientSoftwareName    string
	ClientSoftwareVersion string
	DialTimeout           time.Duration
	RequestTimeout        time.Duration
	Authenticator         auth.Authenticator
	Dialer                *net.Dialer
	SnapshotStorage       SnapshotStorageConfig
	SnapshotFetcher       snapshot.Fetcher
}

func (c Config) withDefaults() Config {
	if c.ClientSoftwareName == "" {
		c.ClientSoftwareName = "fluss-go"
	}
	if c.ClientSoftwareVersion == "" {
		c.ClientSoftwareVersion = "0.1.0"
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 15 * time.Second
	}
	if c.Dialer == nil {
		c.Dialer = &net.Dialer{Timeout: c.DialTimeout}
	}
	if c.Authenticator == nil {
		c.Authenticator = auth.NoopAuthenticator{}
	}
	if c.SnapshotStorage.S3Region == "" {
		c.SnapshotStorage.S3Region = "us-east-1"
	}
	return c
}
