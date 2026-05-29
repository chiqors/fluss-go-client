package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/chiqors/fluss-go-client/client"
	"github.com/chiqors/fluss-go-client/protocol"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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

	names, summaries, err := admin.ListDatabases(ctx, true)
	if err != nil {
		fatalf("list databases: %v", err)
	}
	if len(names) == 0 {
		fmt.Fprintf(os.Stderr, "warning: ListDatabases returned no names; continuing with direct existence checks\n")
	}
	if len(summaries) == 0 {
		fmt.Fprintf(os.Stderr, "warning: ListDatabases returned no summaries; continuing with direct existence checks\n")
	}

	exists, err := admin.DatabaseExists(ctx, database)
	if err != nil {
		fatalf("database exists check failed: %v", err)
	}
	if !exists {
		fatalf("expected database %s to exist", database)
	}

	tables, err := admin.ListTables(ctx, database)
	if err != nil {
		fatalf("list tables failed: %v", err)
	}
	if len(tables) == 0 {
		fmt.Fprintf(os.Stderr, "warning: ListTables returned no names for database %s; continuing with direct table checks\n", database)
	} else {
		assertContains(tables, logTable, "table list")
		assertContains(tables, kvTable, "table list")
	}

	logPath := client.TablePath{DatabaseName: database, TableName: logTable}
	kvPath := client.TablePath{DatabaseName: database, TableName: kvTable}

	checkTableExists(ctx, admin, logPath)
	checkTableExists(ctx, admin, kvPath)

	checkTableMetadata(ctx, cli, logPath)
	checkTableMetadata(ctx, cli, kvPath)

	logPartitions, err := admin.ListPartitionInfos(ctx, logPath)
	if err != nil && !isNonPartitionedTableError(err) {
		fatalf("list log table partitions: %v", err)
	}
	if err == nil && len(logPartitions) != 0 {
		fatalf("expected non-partitioned log table to report zero partition infos, got %d", len(logPartitions))
	}

	kvPartitions, err := admin.ListPartitionInfos(ctx, kvPath)
	if err != nil && !isNonPartitionedTableError(err) {
		fatalf("list kv table partitions: %v", err)
	}
	if err == nil && len(kvPartitions) != 0 {
		fatalf("expected non-partitioned kv table to report zero partition infos, got %d", len(kvPartitions))
	}

	limitResult, err := cli.Table(logPath).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		fatalf("log table limit scan failed: %v", err)
	}
	if !limitResult.IsLogTable {
		fatalf("expected %s.%s to be reported as a log table", database, logTable)
	}

	scanner := cli.Table(kvPath).NewKVScanner(nil, 0, nil, 1024)
	firstBatch, err := scanner.Next(ctx)
	if err != nil {
		fatalf("kv scanner first batch failed: %v", err)
	}
	if len(firstBatch.ScannerID) == 0 {
		fatalf("expected kv scanner to receive a scanner id")
	}
	if err := scanner.Close(ctx); err != nil {
		fatalf("kv scanner close failed: %v", err)
	}

	fmt.Printf("E2E OK: database=%s logTable=%s kvTable=%s limitScanLog=%t kvScannerStarted=%t\n",
		database,
		logTable,
		kvTable,
		limitResult.IsLogTable,
		len(firstBatch.ScannerID) > 0,
	)
}

func checkTableExists(ctx context.Context, admin *client.AdminClient, path client.TablePath) {
	exists, err := admin.TableExists(ctx, path)
	if err != nil {
		fatalf("table exists check failed for %s.%s: %v", path.DatabaseName, path.TableName, err)
	}
	if !exists {
		fatalf("expected table %s.%s to exist", path.DatabaseName, path.TableName)
	}
}

func checkTableMetadata(ctx context.Context, cli *client.Client, path client.TablePath) {
	table := cli.Table(path)

	info, err := table.Info(ctx)
	if err != nil {
		fatalf("get table info for %s.%s: %v", path.DatabaseName, path.TableName, err)
	}
	if info.ID < 0 {
		fatalf("expected non-negative table id for %s.%s, got %d", path.DatabaseName, path.TableName, info.ID)
	}

	schema, err := table.Schema(ctx, nil)
	if err != nil {
		fatalf("get schema for %s.%s: %v", path.DatabaseName, path.TableName, err)
	}
	if schema.SchemaID <= 0 {
		fatalf("expected positive schema id for %s.%s, got %d", path.DatabaseName, path.TableName, schema.SchemaID)
	}
}

func assertContains(values []string, expected, label string) {
	for _, value := range values {
		if value == expected {
			return
		}
	}
	fatalf("expected %q in %s, got %v", expected, label, values)
}

func isNonPartitionedTableError(err error) bool {
	var apiErr *protocol.APIError
	return errors.As(err, &apiErr) && apiErr.Code == 37
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
