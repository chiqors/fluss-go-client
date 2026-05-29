# fluss-client-go

Pure Go Fluss client SDK foundation for Fluss `0.9`.

Current implementation status:

- Native TCP RPC framing and request multiplexing
- Runtime loading of Fluss protobuf descriptors from embedded `.proto`
- API version negotiation and pluggable auth hook
- Metadata cache and bucket leader routing
- Admin APIs for database/table/schema/partition metadata
- Raw table operations for append, upsert, lookup, prefix lookup, fetch log, limit scan, and KV scan

The current data APIs operate on Fluss wire-format record batches as raw bytes. Arrow-first row
encoders/decoders and richer typed row helpers are intentionally left as the next layer on top of
this foundation.

## Example

```go
ctx := context.Background()
cli, err := client.Dial(ctx, client.Config{
	Endpoints: []string{"127.0.0.1:9123"},
})
if err != nil {
	panic(err)
}
defer cli.Close()

admin := cli.Admin()
_, _, err = admin.ListDatabases(ctx, false)
if err != nil {
	panic(err)
}
```
