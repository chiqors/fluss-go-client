package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/chiqors/fluss-go-client/client"
	rowcodec "github.com/chiqors/fluss-go-client/internal/codec/row"
)

type supportStatus string

const (
	supportImplemented supportStatus = "implemented"
	supportPartial     supportStatus = "partial"
	supportMissing     supportStatus = "missing"
)

type featureCase struct {
	section string
	name    string
	status  supportStatus
	run     func(context.Context, *client.Client, environment) error
}

type environment struct {
	database    string
	logTable    client.TablePath
	kvTable     client.TablePath
	prefixTable client.TablePath
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	env := environment{
		database: getenv("FLUSS_DATABASE", "fluss"),
		logTable: client.TablePath{
			DatabaseName: getenv("FLUSS_DATABASE", "fluss"),
			TableName:    getenv("FLUSS_LOG_TABLE", "e2e_orders"),
		},
		kvTable: client.TablePath{
			DatabaseName: getenv("FLUSS_DATABASE", "fluss"),
			TableName:    getenv("FLUSS_KV_TABLE", "e2e_customers"),
		},
		prefixTable: client.TablePath{
			DatabaseName: getenv("FLUSS_DATABASE", "fluss"),
			TableName:    getenv("FLUSS_PREFIX_TABLE", "e2e_customer_orders"),
		},
	}

	cli, err := client.Dial(ctx, client.Config{Endpoints: []string{getenv("FLUSS_BOOTSTRAP", "coordinator-server:9123")}})
	if err != nil {
		fatalf("dial fluss: %v", err)
	}
	defer func() { _ = cli.Close() }()

	admin := cli.Admin()
	if err := waitForTables(ctx, admin, env.database, []string{env.logTable.TableName, env.kvTable.TableName, env.prefixTable.TableName}); err != nil {
		fatalf("wait for bootstrap tables: %v", err)
	}

	checks := []featureCase{
		{
			section: "Admin",
			name:    "ListDatabases",
			status:  supportImplemented,
			run: func(ctx context.Context, cli *client.Client, env environment) error {
				names, summaries, err := cli.Admin().ListDatabases(ctx, true)
				if err != nil {
					return fmt.Errorf("list databases: %w", err)
				}
				if len(names) == 0 {
					fmt.Printf("ListDatabases: empty result on this cluster path\n")
					return nil
				}
				fmt.Printf("ListDatabases: %d databases (%d summaries)\n", len(names), len(summaries))
				return nil
			},
		},
		{
			section: "Admin",
			name:    "DatabaseExists",
			status:  supportImplemented,
			run: func(ctx context.Context, cli *client.Client, env environment) error {
				exists, err := cli.Admin().DatabaseExists(ctx, env.database)
				if err != nil {
					return fmt.Errorf("database exists: %w", err)
				}
				if !exists {
					return fmt.Errorf("database %q not reported as existing", env.database)
				}
				fmt.Printf("DatabaseExists(%s): %v\n", env.database, exists)
				return nil
			},
		},
		{
			section: "Admin",
			name:    "ListTables",
			status:  supportImplemented,
			run: func(ctx context.Context, cli *client.Client, env environment) error {
				tables, err := cli.Admin().ListTables(ctx, env.database)
				if err != nil {
					return fmt.Errorf("list tables: %w", err)
				}
				sort.Strings(tables)
				fmt.Printf("ListTables: %d tables (%v)\n", len(tables), tables)
				return nil
			},
		},
		{
			section: "Metadata",
			name:    "TableInfoAndSchema",
			status:  supportImplemented,
			run: func(ctx context.Context, cli *client.Client, env environment) error {
				for _, path := range []client.TablePath{env.logTable, env.kvTable, env.prefixTable} {
					info, err := cli.Table(path).Info(ctx)
					if err != nil {
						return fmt.Errorf("table %s.%s info: %w", path.DatabaseName, path.TableName, err)
					}
					schema, err := cli.Table(path).Schema(ctx, nil)
					if err != nil {
						return fmt.Errorf("table %s.%s schema: %w", path.DatabaseName, path.TableName, err)
					}
					fmt.Printf("Table %s.%s: ID=%d SchemaID=%d\n", path.DatabaseName, path.TableName, info.ID, schema.SchemaID)
				}
				return nil
			},
		},
		{
			section: "Data Operations",
			name:    "AppendLog",
			status:  supportImplemented,
			run:     runAppendLog,
		},
		{
			section: "Data Operations",
			name:    "Lookup",
			status:  supportImplemented,
			run:     runLookup,
		},
		{
			section: "Data Operations",
			name:    "PrefixLookup",
			status:  supportImplemented,
			run:     runPrefixLookup,
		},
		{
			section: "Data Operations",
			name:    "LimitScan",
			status:  supportImplemented,
			run:     runLimitScan,
		},
		{
			section: "Data Operations",
			name:    "KVScannerLifecycle",
			status:  supportPartial,
			run:     runKVScannerLifecycle,
		},
		{
			section: "Data Types",
			name:    "IndexedRowEncoding",
			status:  supportImplemented,
			run:     runIndexedRowEncoding,
		},
	}

	for _, check := range checks {
		if err := executeFeature(ctx, cli, env, check); err != nil {
			if check.status == supportPartial {
				fmt.Fprintf(os.Stderr, "%s/%s: %v\n", check.section, check.name, err)
				continue
			}
			fatalf("%s/%s: %v", check.section, check.name, err)
		}
	}

	fmt.Printf("\n=== E2E Complete ===\n")
}

func executeFeature(ctx context.Context, cli *client.Client, env environment, check featureCase) error {
	fmt.Printf("[%s] %s (%s)\n", check.section, check.name, check.status)
	if check.status == supportMissing {
		return fmt.Errorf("check is marked missing in the support matrix")
	}
	if check.run == nil {
		return fmt.Errorf("missing runner for %s/%s", check.section, check.name)
	}
	return check.run(ctx, cli, env)
}

func runAppendLog(ctx context.Context, cli *client.Client, env environment) error {
	logSchema := rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.Int32Type(), rowcodec.Float64Type(), rowcodec.StringType())
	logFields := []string{"order_id", "customer_id", "amount", "status"}
	rows := mustRows(logSchema, [][]any{
		{int64(1001), int32(42), 19.95, "created"},
		{int64(1002), int32(43), 29.50, "confirmed"},
	})
	for i, row := range rows {
		appendResult, err := cli.Table(env.logTable).AppendIndexedRow(ctx, 0, row)
		if err != nil {
			return fmt.Errorf("append log row %d: %w", i, err)
		}
		if len(appendResult) != 1 {
			return fmt.Errorf("append log row %d: unexpected result count %d", i, len(appendResult))
		}
		fmt.Printf("AppendLog[%d]: bucket=%d base_offset=%d\n", i, appendResult[0].BucketID, appendResult[0].BaseOffset)
	}

	limitResult, err := cli.Table(env.logTable).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		return fmt.Errorf("limit scan after append: %w", err)
	}
	decoded, err := rowcodec.DecodeLogRecordBatchRows(logSchema, limitResult.Records)
	if err != nil {
		return fmt.Errorf("decode appended log rows: %w", err)
	}
	if len(decoded) < len(rows) {
		return fmt.Errorf("limit scan: got %d rows, want at least %d", len(decoded), len(rows))
	}
	for i, row := range decoded[:len(rows)] {
		if !rowsEqual(row, rows[i].Values) {
			return fmt.Errorf("limit scan row %d: got %v, want %v", i, row, rows[i].Values)
		}
		fmt.Printf("LimitScan Row[%d]: %s\n", i, formatRow(logFields, row))
	}
	return nil
}

func runLookup(ctx context.Context, cli *client.Client, env environment) error {
	kvSchema := rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.StringType(), rowcodec.StringType())
	kvRows := mustRows(kvSchema, [][]any{
		{int64(42), "Ada Lovelace", "gold"},
		{int64(43), "Grace Hopper", "platinum"},
	})
	for i, row := range kvRows {
		putResult, err := cli.Table(env.kvTable).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2})
		if err != nil {
			return fmt.Errorf("upsert kv row %d: %w", i, err)
		}
		if len(putResult) != 1 {
			return fmt.Errorf("upsert kv row %d: unexpected result count %d", i, len(putResult))
		}
		fmt.Printf("UpsertKV[%d]: bucket=%d log_end_offset=%d\n", i, putResult[0].BucketID, putResult[0].LogEndOffset)
	}

	lookupReq := client.LookupBucketRequest{BucketID: 0}
	for _, row := range kvRows {
		keyBytes, err := row.EncodeLookupKey(0)
		if err != nil {
			return fmt.Errorf("encode kv lookup key: %w", err)
		}
		lookupReq.Keys = append(lookupReq.Keys, keyBytes)
	}
	lookupResult, err := cli.Table(env.kvTable).Lookup(ctx, []client.LookupBucketRequest{lookupReq}, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("kv lookup: %w", err)
	}
	if len(lookupResult) != 1 || len(lookupResult[0].Values) != len(kvRows) {
		return fmt.Errorf("kv lookup: unexpected result %#v", lookupResult)
	}
	for i, payload := range lookupResult[0].Values {
		decoded, err := client.DecodeIndexedLookupValuePayload(kvSchema, payload)
		if err != nil {
			return fmt.Errorf("decode kv lookup %d: %w", i, err)
		}
		if !rowsEqual(decoded, kvRows[i].Values) {
			return fmt.Errorf("kv lookup row %d: got %v, want %v", i, decoded, kvRows[i].Values)
		}
		fmt.Printf("LookupKV Row[%d]: %s\n", i, formatRow([]string{"customer_id", "customer_name", "customer_tier"}, decoded))
	}
	return nil
}

func runPrefixLookup(ctx context.Context, cli *client.Client, env environment) error {
	prefixSchema := rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.StringType(), rowcodec.Int64Type(), rowcodec.StringType())
	prefixRows := mustRows(prefixSchema, [][]any{
		{int64(1), "aaaaaaaaa", int64(9001), "pending"},
		{int64(1), "aaaaaaaaa", int64(9002), "packed"},
		{int64(2), "aaaaaaaaa", int64(9003), "shipped"},
	})
	prefixPayload, err := prefixRows[0].EncodeLookupKey(0, 1)
	if err != nil {
		return fmt.Errorf("encode prefix key: %w", err)
	}
	for i, row := range prefixRows {
		putResult, err := cli.Table(env.prefixTable).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2, 3})
		if err != nil {
			return fmt.Errorf("upsert prefix row %d: %w", i, err)
		}
		if len(putResult) != 1 {
			return fmt.Errorf("upsert prefix row %d: unexpected result count %d", i, len(putResult))
		}
		fmt.Printf("UpsertPrefix[%d]: bucket=%d log_end_offset=%d\n", i, putResult[0].BucketID, putResult[0].LogEndOffset)
	}

	prefixLookupResult, err := cli.Table(env.prefixTable).PrefixLookup(ctx, []client.LookupBucketRequest{{BucketID: 0, Keys: [][]byte{prefixPayload}}})
	if err != nil {
		return fmt.Errorf("prefix lookup: %w", err)
	}
	if len(prefixLookupResult) != 1 || len(prefixLookupResult[0].Values) != 1 {
		return fmt.Errorf("prefix lookup: unexpected result %#v", prefixLookupResult)
	}
	if len(prefixLookupResult[0].Values[0]) != 2 {
		return fmt.Errorf("prefix lookup: expected two matches, got %#v", prefixLookupResult[0].Values[0])
	}
	for i, payload := range prefixLookupResult[0].Values[0] {
		decoded, err := client.DecodeIndexedLookupValuePayload(prefixSchema, payload)
		if err != nil {
			return fmt.Errorf("decode prefix lookup %d: %w", i, err)
		}
		if !rowsEqual(decoded, prefixRows[i].Values) {
			return fmt.Errorf("prefix lookup row %d: got %v, want %v", i, decoded, prefixRows[i].Values)
		}
		fmt.Printf("PrefixLookup Row[%d]: %s\n", i, formatRow([]string{"customer_id", "customer_name", "order_id", "order_status"}, decoded))
	}
	return nil
}

func runLimitScan(ctx context.Context, cli *client.Client, env environment) error {
	result, err := cli.Table(env.logTable).LimitScan(ctx, nil, 0, 10)
	if err != nil {
		return fmt.Errorf("limit scan: %w", err)
	}
	fmt.Printf("LimitScan IsLogTable=%v Records=%d bytes\n", result.IsLogTable, len(result.Records))
	if !result.IsLogTable {
		return fmt.Errorf("limit scan: expected log table response")
	}
	return nil
}

func runKVScannerLifecycle(ctx context.Context, cli *client.Client, env environment) error {
	stepCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	scanner := cli.Table(env.kvTable).NewKVScanner(nil, 0, nil, 1024)
	var first client.ScanKVResult
	for i := 0; i < 3; i++ {
		result, err := scanner.Next(stepCtx)
		if err != nil {
			return fmt.Errorf("scanner next %d: %w", i, err)
		}
		if i == 0 {
			first = result
			if len(result.ScannerID) == 0 || len(result.Records) == 0 {
				return fmt.Errorf("unexpected first scanner result: %#v", result)
			}
		} else if len(result.ScannerID) > 0 && string(result.ScannerID) != string(first.ScannerID) {
			return fmt.Errorf("scanner id changed between calls: %q -> %q", first.ScannerID, result.ScannerID)
		}
		if !result.HasMoreResults {
			break
		}
	}
	if err := scanner.Close(stepCtx); err != nil {
		return fmt.Errorf("scanner close: %w", err)
	}
	fmt.Printf("KVScanner: started with %d bytes and closed cleanly\n", len(first.Records))
	return nil
}

func runIndexedRowEncoding(ctx context.Context, cli *client.Client, env environment) error {
	_ = ctx
	_ = cli
	_ = env
	row := mustRows(rowcodec.NewSchema(rowcodec.Int64Type(), rowcodec.StringType()), [][]any{{int64(7), "ok"}})[0]
	if _, err := row.EncodeIndexed(); err != nil {
		return fmt.Errorf("encode indexed row: %w", err)
	}
	if _, err := row.EncodeCompacted(); err != nil {
		return fmt.Errorf("encode compacted row: %w", err)
	}
	return nil
}

func mustRows(schema rowcodec.Schema, values [][]any) []rowcodec.Row {
	rows := make([]rowcodec.Row, 0, len(values))
	for _, rowValues := range values {
		row, err := rowcodec.NewRow(schema, rowValues...)
		if err != nil {
			fatalf("build row: %v", err)
		}
		rows = append(rows, row)
	}
	return rows
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
