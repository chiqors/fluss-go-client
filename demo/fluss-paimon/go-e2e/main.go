package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chiqors/fluss-go-client/client"
	"github.com/chiqors/fluss-go-client/data"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	bootstrap := getenv("FLUSS_BOOTSTRAP", "coordinator-server:9123")
	database := getenv("FLUSS_DATABASE", "fluss")
	logTable := getenv("FLUSS_LOG_TABLE", "e2e_orders")
	kvTable := getenv("FLUSS_KV_TABLE", "e2e_customers")

	cli, err := client.Dial(ctx, client.Config{
		Endpoints: []string{bootstrap},
	})
	if err != nil {
		fatalf("dial fluss: %v", err)
	}
	defer func() { _ = cli.Close() }()

	admin := cli.Admin()

	// Just test reads - existing tables come from SQL bootstrap
	logPath := client.TablePath{DatabaseName: database, TableName: logTable}
	kvPath := client.TablePath{DatabaseName: database, TableName: kvTable}

	if err := waitForTables(ctx, admin, database, []string{logTable, kvTable}); err != nil {
		fatalf("wait for bootstrap tables: %v", err)
	}

	// Test admin APIs
	names, _, err := admin.ListDatabases(ctx, true)
	if err != nil {
		fatalf("list databases: %v", err)
	}
	fmt.Printf("ListDatabases: %d databases\n", len(names))

	exists, err := admin.DatabaseExists(ctx, database)
	if err != nil {
		fatalf("database exists: %v", err)
	}
	fmt.Printf("DatabaseExists(%s): %v\n", database, exists)

	tables, err := admin.ListTables(ctx, database)
	if err != nil {
		fatalf("list tables: %v", err)
	}
	fmt.Printf("ListTables: %d tables (%v)\n", len(tables), tables)

	// Table Info + Schema
	for _, path := range []client.TablePath{logPath, kvPath} {
		info, err := cli.Table(path).Info(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Table %s.%s Info: %v\n", path.DatabaseName, path.TableName, err)
			continue
		}
		schema, err := cli.Table(path).Schema(ctx, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Table %s.%s Schema: %v\n", path.DatabaseName, path.TableName, err)
			continue
		}
		fmt.Printf("Table %s.%s: ID=%d SchemaID=%d\n", path.DatabaseName, path.TableName, info.ID, schema.SchemaID)
	}

	logSchema := data.NewSchema(data.Int64Type(), data.Int32Type(), data.Float64Type(), data.StringType())
	logFields := []string{"order_id", "customer_id", "amount", "status"}
	logRow, err := data.NewRow(logSchema, int64(1001), int32(42), 19.95, "created")
	if err != nil {
		fatalf("build log row: %v", err)
	}
	appendResult, err := cli.Table(logPath).AppendIndexedRow(ctx, 0, logRow)
	if err != nil {
		fatalf("append log row: %v", err)
	}
	if len(appendResult) != 1 {
		fatalf("append log result: unexpected result count %d", len(appendResult))
	}
	fmt.Printf("AppendLog: bucket=%d base_offset=%d\n", appendResult[0].BucketID, appendResult[0].BaseOffset)

	kvSchema := data.NewSchema(data.Int64Type(), data.StringType(), data.StringType())
	kvRow, err := data.NewRow(kvSchema, int64(42), "Ada Lovelace", "gold")
	if err != nil {
		fatalf("build kv row: %v", err)
	}
	putResult, err := cli.Table(kvPath).UpsertIndexedRow(ctx, 0, kvRow, []int32{0, 1, 2})
	if err != nil {
		fatalf("upsert kv row: %v", err)
	}
	if len(putResult) != 1 {
		fatalf("upsert kv result: unexpected result count %d", len(putResult))
	}
	fmt.Printf("UpsertKV: bucket=%d log_end_offset=%d\n", putResult[0].BucketID, putResult[0].LogEndOffset)

	// LimitScan should now have real payload
	limitResult, err := cli.Table(logPath).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		fatalf("limit scan: %v", err)
	}
	fmt.Printf("LimitScan IsLogTable=%v Records=%d bytes\n", limitResult.IsLogTable, len(limitResult.Records))

	decodedLog, err := client.DecodeIndexedLogBatchPayload(logSchema, limitResult.Records)
	if err != nil {
		fatalf("decode log scan: %v", err)
	}
	fmt.Printf("LimitScan Row: %s\n", formatRow(logFields, decodedLog))

	// Verify the KV write with a supported lookup path on this server branch.
	keyBytes, err := kvRow.EncodeLookupKey(0)
	if err != nil {
		fatalf("encode kv lookup key: %v", err)
	}
	lookupResult, err := cli.Table(kvPath).Lookup(ctx, []client.LookupBucketRequest{{BucketID: 0, Keys: [][]byte{keyBytes}}}, nil, nil, nil)
	if err != nil {
		fatalf("kv lookup: %v", err)
	}
	if len(lookupResult) != 1 || len(lookupResult[0].Values) != 1 {
		fatalf("kv lookup: unexpected result %#v", lookupResult)
	}
	decodedKV, err := client.DecodeIndexedLookupValuePayload(kvSchema, lookupResult[0].Values[0])
	if err != nil {
		fatalf("decode kv lookup: %v", err)
	}
	expectedKV := []any{int64(42), "Ada Lovelace", "gold"}
	if len(decodedKV) != len(expectedKV) {
		fatalf("kv lookup: unexpected decoded row length %d, want %d", len(decodedKV), len(expectedKV))
	}
	for i := range expectedKV {
		if decodedKV[i] != expectedKV[i] {
			fatalf("kv lookup: field %d = %v, want %v", i, decodedKV[i], expectedKV[i])
		}
	}
	fmt.Printf("LookupKV Row: %s\n", formatRow([]string{"customer_id", "customer_name", "customer_tier"}, decodedKV))

	fmt.Printf("\n=== E2E Complete ===\n")
}

func getenv(k, f string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return f
}

func waitForTables(ctx context.Context, admin *client.AdminClient, database string, expected []string) error {
	deadline, ok := ctx.Deadline()
	if ok {
		fmt.Fprintf(os.Stderr, "waiting for bootstrap tables until %s\n", deadline.Format(time.RFC3339))
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		tables, err := admin.ListTables(ctx, database)
		if err != nil {
			fmt.Fprintf(os.Stderr, "waiting for bootstrap tables: list tables failed: %v\n", err)
			return err
		}
		fmt.Printf("WaitingForTables: have=%v expected=%v\n", tables, expected)
		if len(tables) >= len(expected) {
			present := map[string]struct{}{}
			for _, name := range tables {
				present[name] = struct{}{}
			}
			matched := true
			for _, name := range expected {
				if _, ok := present[name]; !ok {
					matched = false
					break
				}
			}
			if matched {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func formatRow(fields []string, values []any) string {
	if len(fields) != len(values) {
		return fmt.Sprintf("<mismatched row len fields=%d values=%d>", len(fields), len(values))
	}
	parts := make([]string, 0, len(fields))
	for i := range fields {
		parts = append(parts, fmt.Sprintf("%s=%v", fields[i], values[i]))
	}
	return "{" + joinParts(parts) + "}"
}

func joinParts(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += ", " + parts[i]
	}
	return out
}
