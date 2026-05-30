package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/chiqors/fluss-go-client/internal/metadata"
	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"github.com/chiqors/fluss-go-client/internal/transport"
	"google.golang.org/protobuf/proto"
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
	msg, err := c.rpc.Invoke(ctx, addr, flusspb.ApiKey_GetMetadata, "MetadataRequest", "MetadataResponse", func(m proto.Message) error {
		req, ok := m.(*flusspb.MetadataRequest)
		if !ok {
			return fmt.Errorf("fluss: unexpected metadata request type %T", m)
		}
		for _, path := range tablePaths {
			req.TablePath = append(req.TablePath, &flusspb.PbTablePath{
				DatabaseName: proto.String(path.DatabaseName),
				TableName:    proto.String(path.TableName),
			})
		}
		req.PartitionsId = append(req.PartitionsId, partitionIDs...)
		return nil
	})
	if err != nil {
		return err
	}
	return c.ingestMetadata(msg)
}

func (c *Client) ingestMetadata(message proto.Message) error {
	resp, ok := message.(*flusspb.MetadataResponse)
	if !ok {
		return fmt.Errorf("fluss: unexpected metadata response type %T", message)
	}
	if resp.CoordinatorServer != nil {
		c.metadata.SetCoordinator(parseServerNode(resp.GetCoordinatorServer()))
	}
	for _, node := range resp.GetTabletServers() {
		c.metadata.UpsertServer(parseServerNode(node))
	}
	for _, tableMeta := range resp.GetTableMetadata() {
		info, routes, err := parseTableMetadata(tableMeta)
		if err != nil {
			return err
		}
		c.metadata.SetTable(info)
		for _, route := range routes {
			c.metadata.SetRoute(route)
		}
	}
	for _, partitionMeta := range resp.GetPartitionMetadata() {
		routes, err := parsePartitionMetadata(partitionMeta)
		if err != nil {
			return err
		}
		for _, route := range routes {
			c.metadata.SetRoute(route)
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
