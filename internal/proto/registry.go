package proto

import (
	"fmt"
	"sync"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	once  sync.Once
	fd    protoreflect.FileDescriptor
	fdErr error
	index map[string]protoreflect.MessageDescriptor
)

func fileDescriptor() (protoreflect.FileDescriptor, error) {
	once.Do(func() {
		fd = flusspb.File_fluss_proto
		index = map[string]protoreflect.MessageDescriptor{}
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			register(msgs.Get(i))
		}
	})
	return fd, fdErr
}

func register(md protoreflect.MessageDescriptor) {
	index[string(md.Name())] = md
	for i := 0; i < md.Messages().Len(); i++ {
		register(md.Messages().Get(i))
	}
}

func MessageDescriptor(name string) (protoreflect.MessageDescriptor, error) {
	if _, err := fileDescriptor(); err != nil {
		return nil, err
	}
	md, ok := index[name]
	if !ok {
		return nil, fmt.Errorf("message descriptor not found: %s", name)
	}
	return md, nil
}

func NewMessage(name string) (*dynamicpb.Message, error) {
	md, err := MessageDescriptor(name)
	if err != nil {
		return nil, err
	}
	return dynamicpb.NewMessage(md), nil
}
