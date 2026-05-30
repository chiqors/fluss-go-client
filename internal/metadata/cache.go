package metadata

import (
	"fmt"
	"sync"
)

type TablePath struct {
	DatabaseName string
	TableName    string
}

type ServerNode struct {
	ID        int32
	Host      string
	Port      int32
	Listeners string
	Rack      string
}

func (n ServerNode) Address() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

type TableInfo struct {
	Path          TablePath
	ID            int64
	SchemaID      int32
	TableJSON     []byte
	CreatedTime   int64
	ModifiedTime  int64
	RemoteDataDir string
}

type BucketRoute struct {
	TableID      int64
	PartitionID  int64
	HasPartition bool
	BucketID     int32
	LeaderID     int32
	LeaderEpoch  int32
	Replicas     []int32
}

type Cache struct {
	mu          sync.RWMutex
	coordinator *ServerNode
	servers     map[int32]ServerNode
	tables      map[TablePath]TableInfo
	routes      map[string]BucketRoute
	partitions  map[int64]map[int64]string
}

func NewCache() *Cache {
	return &Cache{
		servers:    map[int32]ServerNode{},
		tables:     map[TablePath]TableInfo{},
		routes:     map[string]BucketRoute{},
		partitions: map[int64]map[int64]string{},
	}
}

func routeKey(tableID int64, partitionID int64, hasPartition bool, bucketID int32) string {
	if hasPartition {
		return fmt.Sprintf("%d/%d/%d", tableID, partitionID, bucketID)
	}
	return fmt.Sprintf("%d/-/%d", tableID, bucketID)
}

func (c *Cache) Coordinator() (ServerNode, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.coordinator == nil {
		return ServerNode{}, false
	}
	return *c.coordinator, true
}

func (c *Cache) SetCoordinator(node ServerNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := node
	c.coordinator = &n
	c.servers[node.ID] = node
}

func (c *Cache) UpsertServer(node ServerNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.servers[node.ID] = node
}

func (c *Cache) Server(id int32) (ServerNode, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, ok := c.servers[id]
	return node, ok
}

func (c *Cache) SetTable(info TableInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[info.Path] = info
}

func (c *Cache) Table(path TablePath) (TableInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.tables[path]
	return info, ok
}

func (c *Cache) SetRoute(route BucketRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes[routeKey(route.TableID, route.PartitionID, route.HasPartition, route.BucketID)] = route
}

func (c *Cache) Route(tableID int64, partitionID int64, hasPartition bool, bucketID int32) (BucketRoute, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	route, ok := c.routes[routeKey(tableID, partitionID, hasPartition, bucketID)]
	return route, ok
}
