package protocol

import (
	"errors"
	"fmt"
)

var (
	ErrUnsupportedVersion = errors.New("fluss: unsupported API version")
	ErrLeaderNotAvailable = errors.New("fluss: leader not available")
	ErrTableNotExist      = errors.New("fluss: table does not exist")
	ErrDatabaseNotExist   = errors.New("fluss: database does not exist")
	ErrAuthentication     = errors.New("fluss: authentication failed")
	ErrTimeout            = errors.New("fluss: request timeout")
	ErrScannerExpired     = errors.New("fluss: scanner expired")
	ErrUnknownServer      = errors.New("fluss: unknown server")
)

type APIError struct {
	Code    int32
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("fluss API error code=%d", e.Code)
	}
	return fmt.Sprintf("fluss API error code=%d: %s", e.Code, e.Message)
}

func (e *APIError) Unwrap() error {
	switch e.Code {
	case 2:
		return ErrUnsupportedVersion
	case 4:
		return ErrDatabaseNotExist
	case 7:
		return ErrTableNotExist
	case 25:
		return ErrTimeout
	case 44:
		return ErrLeaderNotAvailable
	case 46, 51:
		return ErrAuthentication
	case -1:
		return ErrUnknownServer
	case 66:
		return ErrScannerExpired
	default:
		return nil
	}
}
