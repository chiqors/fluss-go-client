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

type featureCase struct {
	section string
	name    string
	run     func(context.Context, *client.Client, environment) error
}

type environment struct {
	database    string
	logTable    client.TablePath
	kvTable     client.TablePath
	prefixTable client.TablePath
}

type decodedRow struct {
	fields []string
	values []any
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	database := getenv("FLUSS_DATABASE", "fluss")
	env := environment{
		database: database,
		logTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_LOG_TABLE", "e2e_orders"),
		},
		kvTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_KV_TABLE", "e2e_customers"),
		},
		prefixTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_PREFIX_TABLE", "e2e_customer_orders"),
		},
	}

	cli, err := client.Dial(ctx, client.Config{Endpoints: []string{getenv("FLUSS_BOOTSTRAP", "coordinator-server:9123")}})
	if err != nil {
		fatalf("dial fluss: %v", err)
	}
	defer func() { _ = cli.Close() }()

	admin := cli.Admin()
	if err := waitForTables(ctx, admin, env.database, []string{
		env.logTable.TableName,
		env.kvTable.TableName,
		env.prefixTable.TableName,
	}); err != nil {
		fatalf("wait for bootstrap tables: %v", err)
	}

	checks := []featureCase{
		{section: "Admin", name: "ListDatabases", run: runListDatabases},
		{section: "Admin", name: "DatabaseExists", run: runDatabaseExists},
		{section: "Admin", name: "ListTables", run: runListTables},
		{section: "Admin", name: "GetTableInfo", run: runGetTableInfo},
		{section: "Admin", name: "GetTableSchema", run: runGetTableSchema},
		{section: "Data Operations", name: "LogAppendAndLimitScan", run: runLogAppendAndLimitScan},
		{section: "Data Operations", name: "PrimaryKeyUpsertAndLookup", run: runPrimaryKeyUpsertAndLookup},
		{section: "Data Operations", name: "PrimaryKeyDelete", run: runPrimaryKeyDelete},
		{section: "Data Operations", name: "PrimaryKeyPrefixLookup", run: runPrimaryKeyPrefixLookup},
	}

	for _, check := range checks {
		fmt.Printf("[%s] %s\n", check.section, check.name)
		if err := check.run(ctx, cli, env); err != nil {
			fatalf("%s/%s: %v", check.section, check.name, err)
		}
		fmt.Printf("[PASS] %s/%s\n", check.section, check.name)
	}

	fmt.Printf("\n=== E2E Complete ===\n")
}

func runListDatabases(ctx context.Context, cli *client.Client, env environment) error {
	names, summaries, err := cli.Admin().ListDatabases(ctx, true)
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	fmt.Printf("ListDatabases: %d databases (%d summaries)\n", len(names), len(summaries))
	return nil
}

func runDatabaseExists(ctx context.Context, cli *client.Client, env environment) error {
	exists, err := cli.Admin().DatabaseExists(ctx, env.database)
	if err != nil {
		return fmt.Errorf("database exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("database %q not reported as existing", env.database)
	}
	fmt.Printf("DatabaseExists(%s): %v\n", env.database, exists)
	return nil
}

func runListTables(ctx context.Context, cli *client.Client, env environment) error {
	tables, err := cli.Admin().ListTables(ctx, env.database)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	sort.Strings(tables)
	required := []string{env.logTable.TableName, env.kvTable.TableName, env.prefixTable.TableName}
	for _, name := range required {
		if !containsString(tables, name) {
			return fmt.Errorf("list tables: missing %q in %v", name, tables)
		}
	}
	fmt.Printf("ListTables: %d tables (%v)\n", len(tables), tables)
	return nil
}

func runGetTableInfo(ctx context.Context, cli *client.Client, env environment) error {
	for _, path := range []client.TablePath{env.logTable, env.kvTable, env.prefixTable} {
		info, err := cli.Table(path).Info(ctx)
		if err != nil {
			return fmt.Errorf("table %s.%s info: %w", path.DatabaseName, path.TableName, err)
		}
		if info.Path.DatabaseName != path.DatabaseName || info.Path.TableName != path.TableName {
			return fmt.Errorf("table %s.%s info: returned mismatched path %+v", path.DatabaseName, path.TableName, info.Path)
		}
		if len(info.JSON) == 0 {
			return fmt.Errorf("table %s.%s info: empty table json", path.DatabaseName, path.TableName)
		}
		fmt.Printf("GetTableInfo %s.%s: ID=%d SchemaID=%d\n", path.DatabaseName, path.TableName, info.ID, info.SchemaID)
	}
	return nil
}

func runGetTableSchema(ctx context.Context, cli *client.Client, env environment) error {
	for _, path := range []client.TablePath{env.logTable, env.kvTable, env.prefixTable} {
		schema, err := cli.Table(path).Schema(ctx, nil)
		if err != nil {
			return fmt.Errorf("table %s.%s schema: %w", path.DatabaseName, path.TableName, err)
		}
		if len(schema.JSON) == 0 {
			return fmt.Errorf("table %s.%s schema: empty schema json", path.DatabaseName, path.TableName)
		}
		fmt.Printf("GetTableSchema %s.%s: SchemaID=%d\n", path.DatabaseName, path.TableName, schema.SchemaID)
	}
	return nil
}

func runLogAppendAndLimitScan(ctx context.Context, cli *client.Client, env environment) error {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.Int32Type(),
		rowcodec.Float64Type(),
		rowcodec.StringType(),
	)
	fields := []string{"order_id", "customer_id", "amount", "status"}
	rows := mustRows(schema, [][]any{
		{int64(2001), int32(101), 19.95, "created"},
		{int64(2002), int32(102), 29.50, "packed"},
		{int64(2003), int32(103), 39.25, "shipped"},
	})

	for i, row := range rows {
		result, err := cli.Table(env.logTable).AppendIndexedRow(ctx, 0, row)
		if err != nil {
			return fmt.Errorf("append log row %d: %w", i, err)
		}
		if len(result) != 1 {
			return fmt.Errorf("append log row %d: unexpected result count %d", i, len(result))
		}
		fmt.Printf("AppendLog[%d]: bucket=%d base_offset=%d\n", i, result[0].BucketID, result[0].BaseOffset)
	}

	limitResult, err := cli.Table(env.logTable).LimitScan(ctx, nil, 0, int32(len(rows)))
	if err != nil {
		return fmt.Errorf("limit scan log table: %w", err)
	}
	if !limitResult.IsLogTable {
		return fmt.Errorf("limit scan log table: expected log-table result")
	}
	decoded, err := rowcodec.DecodeLogRecordBatchRows(schema, limitResult.Records)
	if err != nil {
		return fmt.Errorf("decode log limit scan: %w", err)
	}
	if len(decoded) != len(rows) {
		return fmt.Errorf("limit scan log table: got %d rows, want %d", len(decoded), len(rows))
	}
	for i := range rows {
		if !rowsEqual(decoded[i], rows[i].Values) {
			return fmt.Errorf("limit scan log row %d: got %v, want %v", i, decoded[i], rows[i].Values)
		}
		fmt.Printf("LimitScanLog Row[%d]: %s\n", i, formatRow(fields, decoded[i]))
	}
	return nil
}

func runPrimaryKeyUpsertAndLookup(ctx context.Context, cli *client.Client, env environment) error {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.StringType(),
		rowcodec.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "customer_tier"}
	rows := mustRows(schema, [][]any{
		{int64(42), "Ada Lovelace", "gold"},
		{int64(43), "Grace Hopper", "platinum"},
	})

	for i, row := range rows {
		result, err := cli.Table(env.kvTable).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2})
		if err != nil {
			return fmt.Errorf("upsert kv row %d: %w", i, err)
		}
		if len(result) != 1 {
			return fmt.Errorf("upsert kv row %d: unexpected result count %d", i, len(result))
		}
		fmt.Printf("UpsertKV[%d]: bucket=%d log_end_offset=%d\n", i, result[0].BucketID, result[0].LogEndOffset)
	}

	lookupRows, err := lookupIndexedRows(ctx, cli, env.kvTable, schema, rows, []int{0})
	if err != nil {
		return fmt.Errorf("lookup kv rows: %w", err)
	}
	if len(lookupRows) != len(rows) {
		return fmt.Errorf("lookup kv rows: got %d rows, want %d", len(lookupRows), len(rows))
	}
	for i := range rows {
		if !rowsEqual(lookupRows[i].values, rows[i].Values) {
			return fmt.Errorf("lookup kv row %d: got %v, want %v", i, lookupRows[i].values, rows[i].Values)
		}
		fmt.Printf("LookupKV Row[%d]: %s\n", i, formatRow(fields, lookupRows[i].values))
	}
	return nil
}

func runPrimaryKeyDelete(ctx context.Context, cli *client.Client, env environment) error {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.StringType(),
		rowcodec.StringType(),
	)
	row := mustRows(schema, [][]any{{int64(99), "Delete Me", "silver"}})[0]

	if _, err := cli.Table(env.kvTable).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2}); err != nil {
		return fmt.Errorf("seed delete row: %w", err)
	}
	if _, err := cli.Table(env.kvTable).DeleteIndexedRow(ctx, 0, row, nil); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	found, err := lookupOptionalIndexedRow(ctx, cli, env.kvTable, schema, row, []int{0})
	if err != nil {
		return fmt.Errorf("lookup deleted row: %w", err)
	}
	if found != nil {
		return fmt.Errorf("lookup deleted row: got %v, want nil", found)
	}
	fmt.Printf("DeleteKV: deleted customer_id=%v and lookup returned no row\n", row.Values[0])
	return nil
}

func runPrimaryKeyPrefixLookup(ctx context.Context, cli *client.Client, env environment) error {
	schema := rowcodec.NewSchema(
		rowcodec.Int64Type(),
		rowcodec.StringType(),
		rowcodec.Int64Type(),
		rowcodec.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "order_id", "order_status"}
	rows := mustRows(schema, [][]any{
		{int64(1), "aaaaaaaaa", int64(9001), "pending"},
		{int64(1), "aaaaaaaaa", int64(9002), "packed"},
		{int64(2), "aaaaaaaaa", int64(9003), "shipped"},
	})

	for i, row := range rows {
		result, err := cli.Table(env.prefixTable).UpsertIndexedRow(ctx, 0, row, []int32{0, 1, 2, 3})
		if err != nil {
			return fmt.Errorf("upsert prefix row %d: %w", i, err)
		}
		if len(result) != 1 {
			return fmt.Errorf("upsert prefix row %d: unexpected result count %d", i, len(result))
		}
		fmt.Printf("UpsertPrefix[%d]: bucket=%d log_end_offset=%d\n", i, result[0].BucketID, result[0].LogEndOffset)
	}

	prefixPayload, err := rows[0].EncodeLookupKey(0, 1)
	if err != nil {
		return fmt.Errorf("encode prefix key: %w", err)
	}
	result, err := cli.Table(env.prefixTable).PrefixLookup(ctx, []client.LookupBucketRequest{{
		BucketID: 0,
		Keys:     [][]byte{prefixPayload},
	}})
	if err != nil {
		return fmt.Errorf("prefix lookup: %w", err)
	}
	if len(result) != 1 || len(result[0].Values) != 1 {
		return fmt.Errorf("prefix lookup: unexpected result %#v", result)
	}

	decoded := make([]decodedRow, 0, len(result[0].Values[0]))
	for i, payload := range result[0].Values[0] {
		values, err := client.DecodeIndexedLookupValuePayload(schema, payload)
		if err != nil {
			return fmt.Errorf("decode prefix lookup %d: %w", i, err)
		}
		decoded = append(decoded, decodedRow{fields: fields, values: values})
	}

	want := []decodedRow{
		{fields: fields, values: rows[0].Values},
		{fields: fields, values: rows[1].Values},
	}
	if !sameDecodedRowSet(decoded, want) {
		return fmt.Errorf("prefix lookup rows: got %v, want %v", formatDecodedRows(decoded), formatDecodedRows(want))
	}
	for i, row := range decoded {
		fmt.Printf("PrefixLookup Row[%d]: %s\n", i, formatRow(row.fields, row.values))
	}
	return nil
}

func lookupIndexedRows(ctx context.Context, cli *client.Client, path client.TablePath, schema rowcodec.Schema, rows []rowcodec.Row, keyColumns []int) ([]decodedRow, error) {
	req := client.LookupBucketRequest{BucketID: 0}
	for _, row := range rows {
		key, err := row.EncodeLookupKey(keyColumns...)
		if err != nil {
			return nil, fmt.Errorf("encode lookup key: %w", err)
		}
		req.Keys = append(req.Keys, key)
	}
	result, err := cli.Table(path).Lookup(ctx, []client.LookupBucketRequest{req}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(result) != 1 {
		return nil, fmt.Errorf("unexpected lookup result count %d", len(result))
	}
	if len(result[0].Values) != len(rows) {
		return nil, fmt.Errorf("unexpected lookup value count %d", len(result[0].Values))
	}
	out := make([]decodedRow, 0, len(result[0].Values))
	for _, payload := range result[0].Values {
		if payload == nil {
			out = append(out, decodedRow{})
			continue
		}
		values, err := client.DecodeIndexedLookupValuePayload(schema, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, decodedRow{values: values})
	}
	return out, nil
}

func lookupOptionalIndexedRow(ctx context.Context, cli *client.Client, path client.TablePath, schema rowcodec.Schema, row rowcodec.Row, keyColumns []int) ([]any, error) {
	key, err := row.EncodeLookupKey(keyColumns...)
	if err != nil {
		return nil, fmt.Errorf("encode lookup key: %w", err)
	}
	result, err := cli.Table(path).Lookup(ctx, []client.LookupBucketRequest{{
		BucketID: 0,
		Keys:     [][]byte{key},
	}}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(result) != 1 || len(result[0].Values) != 1 {
		return nil, fmt.Errorf("unexpected lookup result %#v", result)
	}
	if result[0].Values[0] == nil {
		return nil, nil
	}
	values, err := client.DecodeIndexedLookupValuePayload(schema, result[0].Values[0])
	if err != nil {
		return nil, err
	}
	return values, nil
}

func sameDecodedRowSet(got, want []decodedRow) bool {
	if len(got) != len(want) {
		return false
	}
	gotKeys := make([]string, 0, len(got))
	wantKeys := make([]string, 0, len(want))
	for _, row := range got {
		gotKeys = append(gotKeys, rowKey(row.values))
	}
	for _, row := range want {
		wantKeys = append(wantKeys, rowKey(row.values))
	}
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	for i := range gotKeys {
		if gotKeys[i] != wantKeys[i] {
			return false
		}
	}
	return true
}

func rowKey(values []any) string {
	return fmt.Sprintf("%v", values)
}

func formatDecodedRows(rows []decodedRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprintf("%v", row.values))
	}
	sort.Strings(out)
	return out
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
		matched := true
		for _, name := range expected {
			if !containsString(tables, name) {
				matched = false
				break
			}
		}
		if matched {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
