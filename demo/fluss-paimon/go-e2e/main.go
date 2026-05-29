package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chiqors/fluss-go-client/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bootstrap := getenv("FLUSS_BOOTSTRAP", "coordinator-server:9123")
	database := getenv("FLUSS_DATABASE", "fluss")
	table := getenv("FLUSS_TABLE", "e2e_orders")

	cli, err := client.Dial(ctx, client.Config{
		Endpoints: []string{bootstrap},
	})
	if err != nil {
		fatalf("dial fluss: %v", err)
	}
	defer func() { _ = cli.Close() }()

	admin := cli.Admin()
	exists, err := admin.TableExists(ctx, client.TablePath{
		DatabaseName: database,
		TableName:    table,
	})
	if err != nil {
		fatalf("table exists check failed: %v", err)
	}
	if !exists {
		fatalf("expected table %s.%s to exist", database, table)
	}

	info, err := cli.Table(client.TablePath{
		DatabaseName: database,
		TableName:    table,
	}).Info(ctx)
	if err != nil {
		fatalf("get table info: %v", err)
	}
	if info.ID < 0 {
		fatalf("expected non-negative table id, got %d", info.ID)
	}

	schema, err := cli.Table(client.TablePath{
		DatabaseName: database,
		TableName:    table,
	}).Schema(ctx, nil)
	if err != nil {
		fatalf("get schema: %v", err)
	}
	if schema.SchemaID <= 0 {
		fatalf("expected positive schema id, got %d", schema.SchemaID)
	}

	fmt.Printf("E2E OK: table=%s.%s tableID=%d schemaID=%d\n",
		database, table, info.ID, schema.SchemaID)
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
