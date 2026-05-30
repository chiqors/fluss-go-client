package client

import (
	"net"
	"time"

	"github.com/chiqors/fluss-go-client/internal/auth"
)

type Config struct {
	Endpoints             []string
	ClientSoftwareName    string
	ClientSoftwareVersion string
	DialTimeout           time.Duration
	RequestTimeout        time.Duration
	Authenticator         auth.Authenticator
	Dialer                *net.Dialer
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
	return c
}
