package auth

import "context"

type Authenticator interface {
	Protocol() string
	InitialToken(ctx context.Context) ([]byte, error)
	NextToken(ctx context.Context, challenge []byte) ([]byte, bool, error)
}

type NoopAuthenticator struct{}

func (NoopAuthenticator) Protocol() string { return "none" }

func (NoopAuthenticator) InitialToken(context.Context) ([]byte, error) { return nil, nil }

func (NoopAuthenticator) NextToken(context.Context, []byte) ([]byte, bool, error) {
	return nil, false, nil
}
