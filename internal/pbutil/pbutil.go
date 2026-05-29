package pbutil

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func Field(md protoreflect.MessageDescriptor, name string) (protoreflect.FieldDescriptor, error) {
	fd := md.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return nil, fmt.Errorf("field %q not found in %s", name, md.FullName())
	}
	return fd, nil
}

func SetString(msg protoreflect.Message, name, value string) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfString(value))
	return nil
}

func SetBool(msg protoreflect.Message, name string, value bool) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfBool(value))
	return nil
}

func SetInt32(msg protoreflect.Message, name string, value int32) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfInt32(value))
	return nil
}

func SetInt64(msg protoreflect.Message, name string, value int64) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfInt64(value))
	return nil
}

func SetBytes(msg protoreflect.Message, name string, value []byte) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfBytes(value))
	return nil
}

func SetMessage(msg protoreflect.Message, name string, value protoreflect.Message) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	msg.Set(fd, protoreflect.ValueOfMessage(value))
	return nil
}

func AppendString(msg protoreflect.Message, name string, values ...string) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	list := msg.Mutable(fd).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfString(value))
	}
	return nil
}

func AppendInt32(msg protoreflect.Message, name string, values ...int32) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	list := msg.Mutable(fd).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfInt32(value))
	}
	return nil
}

func AppendInt64(msg protoreflect.Message, name string, values ...int64) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	list := msg.Mutable(fd).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfInt64(value))
	}
	return nil
}

func AppendBytes(msg protoreflect.Message, name string, values ...[]byte) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	list := msg.Mutable(fd).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfBytes(value))
	}
	return nil
}

func AppendMessage(msg protoreflect.Message, name string, values ...protoreflect.Message) error {
	fd, err := Field(msg.Descriptor(), name)
	if err != nil {
		return err
	}
	list := msg.Mutable(fd).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfMessage(value))
	}
	return nil
}
