package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/chiqors/fluss-client-go/metadata"
	"github.com/chiqors/fluss-client-go/protocol"
	"github.com/chiqors/fluss-client-go/rpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Client struct {
	cfg       Config
	rpc       *rpc.Client
	metadata  *metadata.Cache
	endpoints []string
}

func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("fluss: at least one endpoint is required")
	}
	r := rpc.NewClient(rpc.Config{
		Dialer:                cfg.Dialer,
		ClientSoftwareName:    cfg.ClientSoftwareName,
		ClientSoftwareVersion: cfg.ClientSoftwareVersion,
		RequestTimeout:        cfg.RequestTimeout,
		Authenticator:         cfg.Authenticator,
	})
	client := &Client{
		cfg:       cfg,
		rpc:       r,
		metadata:  metadata.NewCache(),
		endpoints: append([]string(nil), cfg.Endpoints...),
	}
	if err := client.RefreshMetadata(ctx, nil, nil); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() error {
	return c.rpc.Close()
}

func (c *Client) Admin() *AdminClient {
	return &AdminClient{client: c}
}

func (c *Client) Table(path TablePath) *TableClient {
	return &TableClient{client: c, path: path}
}

func (c *Client) RefreshMetadata(ctx context.Context, tablePaths []TablePath, partitionIDs []int64) error {
	addr := c.endpoints[0]
	if coordinator, ok := c.metadata.Coordinator(); ok {
		addr = coordinator.Address()
	}
	msg, err := c.rpc.Invoke(ctx, addr, protocol.GetMetadata, "MetadataRequest", "MetadataResponse", func(m *dynamicpb.Message) error {
		dm := m.ProtoReflect()
		for _, path := range tablePaths {
			pm, err := buildTablePath(path)
			if err != nil {
				return err
			}
			if err := pbAppendMessage(dm, "table_path", pm); err != nil {
				return err
			}
		}
		return pbAppendInt64(dm, "partitions_id", partitionIDs...)
	})
	if err != nil {
		return err
	}
	return c.ingestMetadata(msg)
}

func (c *Client) ingestMetadata(message protoreflect.ProtoMessage) error {
	resp := message.ProtoReflect()
	if fd := resp.Descriptor().Fields().ByName("coordinator_server"); fd != nil && resp.Has(fd) {
		node, err := parseServerNode(resp.Get(fd).Message())
		if err != nil {
			return err
		}
		c.metadata.SetCoordinator(node)
	}
	if fd := resp.Descriptor().Fields().ByName("tablet_servers"); fd != nil {
		list := resp.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			node, err := parseServerNode(list.Get(i).Message())
			if err != nil {
				return err
			}
			c.metadata.UpsertServer(node)
		}
	}
	if fd := resp.Descriptor().Fields().ByName("table_metadata"); fd != nil {
		list := resp.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			info, routes, err := parseTableMetadata(list.Get(i).Message())
			if err != nil {
				return err
			}
			c.metadata.SetTable(info)
			for _, route := range routes {
				c.metadata.SetRoute(route)
			}
		}
	}
	if fd := resp.Descriptor().Fields().ByName("partition_metadata"); fd != nil {
		list := resp.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			routes, err := parsePartitionMetadata(list.Get(i).Message())
			if err != nil {
				return err
			}
			for _, route := range routes {
				c.metadata.SetRoute(route)
			}
		}
	}
	return nil
}

func (c *Client) routeFor(tableID int64, partitionID *int64, bucketID int32) (metadata.ServerNode, error) {
	var (
		route metadata.BucketRoute
		ok    bool
	)
	if partitionID != nil {
		route, ok = c.metadata.Route(tableID, *partitionID, true, bucketID)
	} else {
		route, ok = c.metadata.Route(tableID, 0, false, bucketID)
	}
	if !ok {
		return metadata.ServerNode{}, fmt.Errorf("fluss: no route for table=%d bucket=%d", tableID, bucketID)
	}
	node, ok := c.metadata.Server(route.LeaderID)
	if !ok {
		return metadata.ServerNode{}, fmt.Errorf("fluss: leader %d not found", route.LeaderID)
	}
	return node, nil
}
