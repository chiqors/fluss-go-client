package proto

import (
	_ "embed"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

//go:embed fluss.proto
var flussProto string

var (
	once  sync.Once
	fd    protoreflect.FileDescriptor
	fdErr error
	index map[string]protoreflect.MessageDescriptor
)

func fileDescriptor() (protoreflect.FileDescriptor, error) {
	once.Do(func() {
		parser := protoparse.Parser{
			Accessor: func(name string) (io.ReadCloser, error) {
				if name != "fluss.proto" {
					return nil, fmt.Errorf("unknown proto %q", name)
				}
				return io.NopCloser(strings.NewReader(flussProto)), nil
			},
		}
		var files []*desc.FileDescriptor
		files, fdErr = parser.ParseFiles("fluss.proto")
		if fdErr != nil {
			return
		}
		if len(files) != 1 {
			fdErr = fmt.Errorf("unexpected file count %d", len(files))
			return
		}
		fd, fdErr = protodesc.NewFile(files[0].AsFileDescriptorProto(), nil)
		if fdErr != nil {
			return
		}
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
