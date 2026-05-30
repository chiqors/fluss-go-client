package metadata

import "testing"

func TestCacheRoute(t *testing.T) {
	cache := NewCache()
	cache.SetRoute(BucketRoute{TableID: 1, BucketID: 2, LeaderID: 3})
	route, ok := cache.Route(1, 0, false, 2)
	if !ok {
		t.Fatal("route not found")
	}
	if route.LeaderID != 3 {
		t.Fatalf("unexpected leader %d", route.LeaderID)
	}
}
