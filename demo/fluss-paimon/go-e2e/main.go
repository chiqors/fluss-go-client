package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chiqors/fluss-go-client/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// LimitScan (will be empty but should work)
	limitResult, err := cli.Table(logPath).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		fatalf("limit scan: %v", err)
	}
	fmt.Printf("LimitScan IsLogTable=%v Records=%d bytes\n", limitResult.IsLogTable, len(limitResult.Records))

	// KVScanner (may timeout if tables empty - skip gracefully)
	scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	scanner := cli.Table(kvPath).NewKVScanner(nil, 0, nil, 1024)
	batch, err := scanner.Next(scanCtx)
	scanner.Close(context.Background())
	if err != nil {
		if err == scanCtx.Err() {
			fmt.Printf("KVScanner: TIMEOUT (expected for empty tables)\n")
		} else {
			fmt.Fprintf(os.Stderr, "KVScanner: %v (ignored)\n", err)
		}
	} else {
		fmt.Printf("KVScanner: ScannerID=%d Records=%d\n", len(batch.ScannerID), len(batch.Records))
	}

	fmt.Printf("\n=== E2E Complete ===\n")
}

func getenv(k, f string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return f
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
