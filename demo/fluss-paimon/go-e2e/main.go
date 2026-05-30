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
	logRows := []data.Row{}
	for _, values := range [][]any{
		{int64(1001), int32(42), 19.95, "created"},
		{int64(1002), int32(43), 29.50, "confirmed"},
	} {
		row, err := data.NewRow(logSchema, values...)
		if err != nil {
			fatalf("build log row: %v", err)
		}
		logRows = append(logRows, row)
	}
	for i, row := range logRows {
		appendResult, err := cli.Table(logPath).AppendIndexedRow(ctx, 0, row)
		if err != nil {
			fatalf("append log row %d: %v", i, err)
		}
		if len(appendResult) != 1 {
			fatalf("append log result %d: unexpected result count %d", i, len(appendResult))
		}
		fmt.Printf("AppendLog[%d]: bucket=%d base_offset=%d\n", i, appendResult[0].BucketID, appendResult[0].BaseOffset)
	}

	kvSchema := data.NewSchema(data.Int64Type(), data.StringType(), data.StringType())
	kvRows := []data.Row{}
	for _, values := range [][]any{
		{int64(42), "Ada Lovelace", "gold"},
		{int64(43), "Grace Hopper", "platinum"},
	} {
		row, err := data.NewRow(kvSchema, values...)
		if err != nil {
			fatalf("build kv row: %v", err)
		}
		kvRows = append(kvRows, row)
	}
	for i, row := range kvRows {
		putResult, err := cli.Table(kvPath).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2})
		if err != nil {
			fatalf("upsert kv row %d: %v", i, err)
		}
		if len(putResult) != 1 {
			fatalf("upsert kv result %d: unexpected result count %d", i, len(putResult))
		}
		fmt.Printf("UpsertKV[%d]: bucket=%d log_end_offset=%d\n", i, putResult[0].BucketID, putResult[0].LogEndOffset)
	}

	// LimitScan should now have real payload
	limitResult, err := cli.Table(logPath).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		fatalf("limit scan: %v", err)
	}
	fmt.Printf("LimitScan IsLogTable=%v Records=%d bytes\n", limitResult.IsLogTable, len(limitResult.Records))

	decodedLogs, err := data.DecodeLogRecordBatchRows(logSchema, limitResult.Records)
	if err != nil {
		fatalf("decode log scan: %v", err)
	}
	if len(decodedLogs) < len(logRows) {
		fatalf("limit scan: got %d rows, want at least %d", len(decodedLogs), len(logRows))
	}
	for i, row := range decodedLogs[:len(logRows)] {
		expected := logRows[i].Values
		if !rowsEqual(row, expected) {
			fatalf("limit scan row %d: got %v, want %v", i, row, expected)
		}
		fmt.Printf("LimitScan Row[%d]: %s\n", i, formatRow(logFields, row))
	}

	// Verify the KV write with a supported lookup path on this server branch.
	lookupReq := client.LookupBucketRequest{BucketID: 0}
	for _, row := range kvRows {
		keyBytes, err := row.EncodeLookupKey(0)
		if err != nil {
			fatalf("encode kv lookup key: %v", err)
		}
		lookupReq.Keys = append(lookupReq.Keys, keyBytes)
	}
	lookupResult, err := cli.Table(kvPath).Lookup(ctx, []client.LookupBucketRequest{lookupReq}, nil, nil, nil)
	if err != nil {
		fatalf("kv lookup: %v", err)
	}
	if len(lookupResult) != 1 || len(lookupResult[0].Values) != len(kvRows) {
		fatalf("kv lookup: unexpected result %#v", lookupResult)
	}
	for i, payload := range lookupResult[0].Values {
		decodedKV, err := client.DecodeIndexedLookupValuePayload(kvSchema, payload)
		if err != nil {
			fatalf("decode kv lookup %d: %v", i, err)
		}
		if !rowsEqual(decodedKV, kvRows[i].Values) {
			fatalf("kv lookup row %d: got %v, want %v", i, decodedKV, kvRows[i].Values)
		}
		fmt.Printf("LookupKV Row[%d]: %s\n", i, formatRow([]string{"customer_id", "customer_name", "customer_tier"}, decodedKV))
	}

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

func rowsEqual(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
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
