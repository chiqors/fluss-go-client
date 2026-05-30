package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/chiqors/fluss-go-client/client"
)

type featureCase struct {
	section string
	name    string
	run     func(context.Context, *client.Client, environment) error
}

type environment struct {
	database         string
	logTable         client.TablePath
	arrowTable       client.TablePath
	kvTable          client.TablePath
	prefixTable      client.TablePath
	typesTable       client.TablePath
	adminPartitioned client.TablePath
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
		arrowTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_ARROW_LOG_TABLE", "e2e_orders_arrow"),
		},
		kvTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_KV_TABLE", "e2e_customers"),
		},
		prefixTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_PREFIX_TABLE", "e2e_customer_orders"),
		},
		typesTable: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_TYPES_TABLE", "e2e_all_types"),
		},
		adminPartitioned: client.TablePath{
			DatabaseName: database,
			TableName:    getenv("FLUSS_ADMIN_PARTITION_TABLE", "e2e_admin_partitions"),
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
		env.arrowTable.TableName,
		env.kvTable.TableName,
		env.prefixTable.TableName,
		env.typesTable.TableName,
		env.adminPartitioned.TableName,
	}); err != nil {
		fatalf("wait for bootstrap tables: %v", err)
	}

	checks := []featureCase{
		{section: "Admin", name: "ListDatabases", run: runListDatabases},
		{section: "Admin", name: "DatabaseExists", run: runDatabaseExists},
		{section: "Admin", name: "GetDatabaseInfo", run: runGetDatabaseInfo},
		{section: "Admin", name: "CreateAndDropDatabase", run: runCreateAndDropDatabase},
		{section: "Admin", name: "ListTables", run: runListTables},
		{section: "Admin", name: "TableExists", run: runTableExists},
		{section: "Admin", name: "GetTableInfo", run: runGetTableInfo},
		{section: "Admin", name: "GetTableSchema", run: runGetTableSchema},
		{section: "Admin", name: "CreateAlterAndDropTable", run: runCreateAlterAndDropTable},
		{section: "Admin", name: "PartitionLifecycle", run: runPartitionLifecycle},
		{section: "Data Operations", name: "LogAppendAndLimitScan", run: runLogAppendAndLimitScan},
		{section: "Data Operations", name: "ArrowLogAppendFetchAndProjection", run: runArrowLogAppendFetchAndProjection},
		{section: "Data Operations", name: "AllTypesAppendAndLimitScan", run: runAllTypesAppendAndLimitScan},
		{section: "Data Operations", name: "PrimaryKeyUpsertAndLookup", run: runPrimaryKeyUpsertAndLookup},
		{section: "Data Operations", name: "PrimaryKeyPartialUpdate", run: runPrimaryKeyPartialUpdate},
		{section: "Data Operations", name: "PrimaryKeyLimitScan", run: runPrimaryKeyLimitScan},
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
	resolvedNames := append([]string(nil), names...)
	if len(resolvedNames) == 0 && len(summaries) > 0 {
		for _, summary := range summaries {
			if summary.DatabaseName != "" {
				resolvedNames = append(resolvedNames, summary.DatabaseName)
			}
		}
	}
	fmt.Printf("ListDatabases: names=%d summaries=%d resolved=%v\n", len(names), len(summaries), resolvedNames)
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

func runGetDatabaseInfo(ctx context.Context, cli *client.Client, env environment) error {
	info, err := cli.Admin().GetDatabaseInfo(ctx, env.database)
	if err != nil {
		return fmt.Errorf("get database info: %w", err)
	}
	if len(info.JSON) == 0 {
		return fmt.Errorf("get database info: empty database json")
	}
	fmt.Printf("GetDatabaseInfo(%s): created=%d modified=%d json_bytes=%d\n", env.database, info.CreatedTime, info.ModifiedTime, len(info.JSON))
	return nil
}

func runCreateAndDropDatabase(ctx context.Context, cli *client.Client, env environment) error {
	name := "go_e2e_admin_db"
	if err := cli.Admin().DropDatabase(ctx, name, true, true); err != nil {
		return fmt.Errorf("cleanup temp database: %w", err)
	}
	if err := cli.Admin().CreateDatabase(ctx, name, []byte(`{"version":1,"comment":"go e2e temp database","custom_properties":{}}`), false); err != nil {
		return fmt.Errorf("create temp database: %w", err)
	}
	exists, err := cli.Admin().DatabaseExists(ctx, name)
	if err != nil {
		return fmt.Errorf("verify temp database exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("temp database %q was not created", name)
	}
	info, err := cli.Admin().GetDatabaseInfo(ctx, name)
	if err != nil {
		return fmt.Errorf("get temp database info: %w", err)
	}
	if len(info.JSON) == 0 {
		return fmt.Errorf("get temp database info: empty database json")
	}
	if err := cli.Admin().DropDatabase(ctx, name, false, true); err != nil {
		return fmt.Errorf("drop temp database: %w", err)
	}
	exists, err = cli.Admin().DatabaseExists(ctx, name)
	if err != nil {
		return fmt.Errorf("verify temp database drop: %w", err)
	}
	if exists {
		return fmt.Errorf("temp database %q still exists after drop", name)
	}
	fmt.Printf("CreateDropDatabase: name=%s lifecycle_ok=true\n", name)
	return nil
}

func runListTables(ctx context.Context, cli *client.Client, env environment) error {
	tables, err := cli.Admin().ListTables(ctx, env.database)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	sort.Strings(tables)
	required := []string{env.logTable.TableName, env.arrowTable.TableName, env.kvTable.TableName, env.prefixTable.TableName, env.typesTable.TableName, env.adminPartitioned.TableName}
	for _, name := range required {
		if !containsString(tables, name) {
			return fmt.Errorf("list tables: missing %q in %v", name, tables)
		}
	}
	fmt.Printf("ListTables: %d tables (%v)\n", len(tables), tables)
	return nil
}

func runTableExists(ctx context.Context, cli *client.Client, env environment) error {
	for _, path := range []client.TablePath{env.logTable, env.arrowTable, env.kvTable, env.prefixTable, env.typesTable, env.adminPartitioned} {
		exists, err := cli.Admin().TableExists(ctx, path)
		if err != nil {
			return fmt.Errorf("table exists %s.%s: %w", path.DatabaseName, path.TableName, err)
		}
		if !exists {
			return fmt.Errorf("table exists %s.%s: expected true", path.DatabaseName, path.TableName)
		}
		fmt.Printf("TableExists(%s.%s): %v\n", path.DatabaseName, path.TableName, exists)
	}
	return nil
}

func runGetTableInfo(ctx context.Context, cli *client.Client, env environment) error {
	for _, path := range []client.TablePath{env.logTable, env.arrowTable, env.kvTable, env.prefixTable, env.typesTable, env.adminPartitioned} {
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
	for _, path := range []client.TablePath{env.logTable, env.arrowTable, env.kvTable, env.prefixTable, env.typesTable, env.adminPartitioned} {
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

func runCreateAlterAndDropTable(ctx context.Context, cli *client.Client, env environment) error {
	path := client.TablePath{DatabaseName: env.database, TableName: "go_e2e_admin_table"}
	if err := cli.Admin().DropTable(ctx, path, true); err != nil {
		return fmt.Errorf("cleanup temp table: %w", err)
	}
	createJSON := []byte(`{
		"version":1,
		"schema":{
			"version":1,
			"columns":[
				{"name":"id","data_type":{"type":"BIGINT","nullable":true},"id":0},
				{"name":"name","data_type":{"type":"STRING","nullable":true},"id":1}
			],
			"highest_field_id":1
		},
		"comment":"go e2e temp table",
		"partition_key":[],
		"bucket_key":[],
		"bucket_count":1,
		"properties":{},
		"custom_properties":{}
	}`)
	if err := cli.Admin().CreateTable(ctx, path, createJSON, false); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}
	exists, err := cli.Admin().TableExists(ctx, path)
	if err != nil {
		return fmt.Errorf("verify temp table exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("temp table %s.%s was not created", path.DatabaseName, path.TableName)
	}
	value := "300s"
	if err := cli.Admin().AlterTable(ctx, path, []client.AlterTableChange{
		client.TableConfigChange{Key: "client.connect-timeout", Value: &value, Op: client.AlterConfigSet},
	}, false); err != nil {
		return fmt.Errorf("alter temp table: %w", err)
	}
	info, err := cli.Table(path).Info(ctx)
	if err != nil {
		return fmt.Errorf("get temp table info after alter: %w", err)
	}
	if len(info.JSON) == 0 {
		return fmt.Errorf("get temp table info after alter: empty table json")
	}
	if err := cli.Admin().DropTable(ctx, path, false); err != nil {
		return fmt.Errorf("drop temp table: %w", err)
	}
	exists, err = cli.Admin().TableExists(ctx, path)
	if err != nil {
		return fmt.Errorf("verify temp table drop: %w", err)
	}
	if exists {
		return fmt.Errorf("temp table %s.%s still exists after drop", path.DatabaseName, path.TableName)
	}
	fmt.Printf("CreateAlterDropTable: table=%s.%s lifecycle_ok=true\n", path.DatabaseName, path.TableName)
	return nil
}

func runPartitionLifecycle(ctx context.Context, cli *client.Client, env environment) error {
	path := env.adminPartitioned
	spec := client.PartitionSpec{{Key: "pt", Value: "2025"}}
	if err := cli.Admin().DropPartition(ctx, path, spec, true); err != nil {
		return fmt.Errorf("cleanup temp partition: %w", err)
	}
	before, err := cli.Admin().ListPartitionInfos(ctx, path)
	if err != nil {
		return fmt.Errorf("list partition infos before create: %w", err)
	}
	if err := cli.Admin().CreatePartition(ctx, path, spec, false); err != nil {
		return fmt.Errorf("create partition: %w", err)
	}
	after, err := cli.Admin().ListPartitionInfosWithSpec(ctx, path, spec)
	if err != nil {
		return fmt.Errorf("list partition infos with spec after create: %w", err)
	}
	if len(after) == 0 {
		return fmt.Errorf("list partition infos with spec after create: no matching partition returned")
	}
	found := false
	for _, info := range after {
		if samePartitionSpec(info.PartitionSpec, spec) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("created partition spec %v not returned in filtered partition list %v", spec, after)
	}
	if err := cli.Admin().DropPartition(ctx, path, spec, false); err != nil {
		return fmt.Errorf("drop partition: %w", err)
	}
	final, err := cli.Admin().ListPartitionInfosWithSpec(ctx, path, spec)
	if err != nil {
		return fmt.Errorf("list partition infos with spec after drop: %w", err)
	}
	if len(final) != 0 {
		return fmt.Errorf("partition still returned after drop: %v", final)
	}
	fmt.Printf("PartitionLifecycle: table=%s.%s before=%d after_create=%d after_drop=%d\n", path.DatabaseName, path.TableName, len(before), len(after), len(final))
	return nil
}

func runLogAppendAndLimitScan(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.Int32Type(),
		client.Float64Type(),
		client.StringType(),
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
	decoded, err := client.DecodeIndexedLogBatchRows(schema, limitResult.Records)
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

func runArrowLogAppendFetchAndProjection(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.Int32Type(),
		client.Float64Type(),
		client.StringType(),
	)
	projectedSchema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
	)
	fields := []string{"order_id", "customer_id", "amount", "status"}
	projectedFields := []string{"order_id", "status"}
	rows := mustRows(schema, [][]any{
		{int64(4001), int32(201), 11.25, "created"},
		{int64(4002), int32(202), 22.50, "packed"},
	})

	result, err := cli.Table(env.arrowTable).AppendArrowRows(ctx, 0, rows)
	if err != nil {
		return fmt.Errorf("append arrow rows: %w", err)
	}
	if len(result) != 1 {
		return fmt.Errorf("append arrow rows: unexpected result count %d", len(result))
	}
	fmt.Printf("AppendArrowLog: bucket=%d base_offset=%d rows=%d\n", result[0].BucketID, result[0].BaseOffset, len(rows))

	fetched, err := cli.Table(env.arrowTable).FetchLog(ctx, -1, 4096, nil, nil, []client.FetchBucketRequest{{
		BucketID:      0,
		FetchOffset:   result[0].BaseOffset,
		MaxFetchBytes: 4096,
	}})
	if err != nil {
		return fmt.Errorf("fetch arrow log rows: %w", err)
	}
	if len(fetched) != 1 {
		return fmt.Errorf("fetch arrow log rows: unexpected bucket count %d", len(fetched))
	}
	decoded, err := client.DecodeArrowLogBatchRows(schema, fetched[0].Records)
	if err != nil {
		return fmt.Errorf("decode fetched arrow log rows: %w", err)
	}
	if len(decoded) < len(rows) {
		return fmt.Errorf("fetch arrow log rows: got %d rows, want at least %d", len(decoded), len(rows))
	}
	decoded = decoded[:len(rows)]
	for i := range rows {
		if !rowsEqual(decoded[i], rows[i].Values) {
			return fmt.Errorf("fetch arrow row %d: got %v, want %v", i, decoded[i], rows[i].Values)
		}
		fmt.Printf("FetchArrowLog Row[%d]: %s\n", i, formatRow(fields, decoded[i]))
	}

	projected, err := cli.Table(env.arrowTable).FetchLogWithOptions(ctx, -1, 4096, nil, nil, []client.FetchBucketRequest{{
		BucketID:      0,
		FetchOffset:   result[0].BaseOffset,
		MaxFetchBytes: 4096,
	}}, client.FetchLogOptions{ProjectedFields: []int32{0, 3}})
	if err != nil {
		return fmt.Errorf("fetch projected arrow log rows: %w", err)
	}
	if len(projected) != 1 {
		return fmt.Errorf("fetch projected arrow log rows: unexpected bucket count %d", len(projected))
	}
	projectedRows, err := client.DecodeProjectedArrowLogBatchRows(projectedSchema, projected[0].Records)
	if err != nil {
		return fmt.Errorf("decode projected arrow log rows: %w", err)
	}
	if len(projectedRows) < len(rows) {
		return fmt.Errorf("projected arrow log rows: got %d rows, want at least %d", len(projectedRows), len(rows))
	}
	projectedRows = projectedRows[:len(rows)]
	want := [][]any{
		{rows[0].Values[0], rows[0].Values[3]},
		{rows[1].Values[0], rows[1].Values[3]},
	}
	for i := range want {
		if !rowsEqual(projectedRows[i], want[i]) {
			return fmt.Errorf("projected arrow row %d: got %v, want %v", i, projectedRows[i], want[i])
		}
		fmt.Printf("ProjectedArrowLog Row[%d]: %s\n", i, formatRow(projectedFields, projectedRows[i]))
	}
	return nil
}

func runPrimaryKeyUpsertAndLookup(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
		client.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "customer_tier"}
	rows := mustRows(schema, [][]any{
		{int64(42), "Ada Lovelace", "gold"},
		{int64(43), "Grace Hopper", "platinum"},
	})

	for i, row := range rows {
		result, err := cli.Table(env.kvTable).UpsertRow(ctx, 0, row, nil)
		if err != nil {
			return fmt.Errorf("upsert kv row %d: %w", i, err)
		}
		if len(result) != 1 {
			return fmt.Errorf("upsert kv row %d: unexpected result count %d", i, len(result))
		}
		fmt.Printf("UpsertKV[%d]: bucket=%d log_end_offset=%d\n", i, result[0].BucketID, result[0].LogEndOffset)
	}

	lookupRows, err := cli.Table(env.kvTable).LookupRows(ctx, 0, schema, rows, []int{0})
	if err != nil {
		return fmt.Errorf("lookup kv rows: %w", err)
	}
	if len(lookupRows) != len(rows) {
		return fmt.Errorf("lookup kv rows: got %d rows, want %d", len(lookupRows), len(rows))
	}
	for i := range rows {
		if !rowsEqual(lookupRows[i], rows[i].Values) {
			return fmt.Errorf("lookup kv row %d: got %v, want %v", i, lookupRows[i], rows[i].Values)
		}
		fmt.Printf("LookupKV Row[%d]: %s\n", i, formatRow(fields, lookupRows[i]))
	}
	return nil
}

func runAllTypesAppendAndLimitScan(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.BoolType(),
		client.Int8Type(),
		client.Int16Type(),
		client.Int32Type(),
		client.Int64Type(),
		client.Float32Type(),
		client.Float64Type(),
		client.StringType(),
		client.BytesType(),
		client.DecimalType(10, 2),
		client.DateType(),
		client.TimeType(),
		client.TimestampNtzType(6),
		client.TimestampLtzType(6),
		client.ArrayType(client.Int32Type()),
		client.MapType(client.StringType(), client.Int64Type()),
		client.RowType(
			client.StringType(),
			client.Int32Type(),
			client.ArrayType(client.StringType()),
		),
	)
	fields := []string{
		"event_id",
		"bool_flag",
		"tiny_value",
		"small_value",
		"int_value",
		"big_value",
		"float_value",
		"double_value",
		"name",
		"raw_bytes",
		"amount",
		"event_date",
		"event_time",
		"event_ts",
		"event_ts_ltz",
		"score_history",
		"label_counts",
		"nested_payload",
	}
	rows := mustRows(schema, [][]any{
		{
			int64(3001),
			true,
			int8(7),
			int16(70),
			int32(700),
			int64(7000),
			float32(7.25),
			float64(70.5),
			"typed-event",
			[]byte("payload"),
			mustDecimal("123.45", 10, 2),
			int32(20000),
			int32(3723000),
			client.TimestampNtz{Millisecond: 1717000000123, NanoOfMillisecond: 456789},
			client.TimestampLtz{EpochMillisecond: 1717000000456, NanoOfMillisecond: 123456},
			[]any{int32(3), int32(5), int32(8)},
			map[any]any{"alpha": int64(1), "beta": int64(2)},
			[]any{"note", int32(9), []any{"x", "y"}},
		},
	})

	for i, row := range rows {
		result, err := cli.Table(env.typesTable).AppendIndexedRow(ctx, 0, row)
		if err != nil {
			return fmt.Errorf("append all-types row %d: %w", i, err)
		}
		if len(result) != 1 {
			return fmt.Errorf("append all-types row %d: unexpected result count %d", i, len(result))
		}
		fmt.Printf("AppendAllTypes[%d]: bucket=%d base_offset=%d\n", i, result[0].BucketID, result[0].BaseOffset)
	}

	limitResult, err := cli.Table(env.typesTable).LimitScan(ctx, nil, 0, int32(len(rows)))
	if err != nil {
		return fmt.Errorf("limit scan all-types table: %w", err)
	}
	if !limitResult.IsLogTable {
		return fmt.Errorf("limit scan all-types table: expected log-table result")
	}
	decoded, err := client.DecodeIndexedLogBatchRows(schema, limitResult.Records)
	if err != nil {
		return fmt.Errorf("decode all-types log rows: %w", err)
	}
	if len(decoded) != len(rows) {
		return fmt.Errorf("limit scan all-types table: got %d rows, want %d", len(decoded), len(rows))
	}
	for i := range rows {
		if !deepRowsEqual(decoded[i], rows[i].Values) {
			return fmt.Errorf("all-types row %d: got %v, want %v", i, decoded[i], rows[i].Values)
		}
		fmt.Printf("AllTypes Row[%d]: %s\n", i, formatRow(fields, decoded[i]))
	}
	return nil
}

func runPrimaryKeyPartialUpdate(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
		client.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "customer_tier"}
	row := mustRows(schema, [][]any{
		{int64(42), nil, "diamond"},
	})[0]

	result, err := cli.Table(env.kvTable).PartialUpdateRow(ctx, 0, row, []int32{0, 2})
	if err != nil {
		return fmt.Errorf("partial update kv row: %w", err)
	}
	if len(result) != 1 {
		return fmt.Errorf("partial update kv row: unexpected result count %d", len(result))
	}
	fmt.Printf("PartialUpdateKV: bucket=%d log_end_offset=%d target_columns=%v\n", result[0].BucketID, result[0].LogEndOffset, []int32{0, 2})

	lookup, err := cli.Table(env.kvTable).LookupRow(ctx, 0, schema, row, []int{0})
	if err != nil {
		return fmt.Errorf("lookup partial-update row: %w", err)
	}
	want := []any{int64(42), "Ada Lovelace", "diamond"}
	if !rowsEqual(lookup, want) {
		return fmt.Errorf("lookup partial-update row: got %v, want %v", lookup, want)
	}
	fmt.Printf("PartialUpdateKV Lookup: %s\n", formatRow(fields, lookup))
	return nil
}

func runPrimaryKeyDelete(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
		client.StringType(),
	)
	row := mustRows(schema, [][]any{{int64(99), "Delete Me", "silver"}})[0]

	if _, err := cli.Table(env.kvTable).UpsertRow(ctx, 0, row, nil); err != nil {
		return fmt.Errorf("seed delete row: %w", err)
	}
	if _, err := cli.Table(env.kvTable).DeleteRow(ctx, 0, row, nil); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	found, err := cli.Table(env.kvTable).LookupRow(ctx, 0, schema, row, []int{0})
	if err != nil {
		return fmt.Errorf("lookup deleted row: %w", err)
	}
	if found != nil {
		return fmt.Errorf("lookup deleted row: got %v, want nil", found)
	}
	fmt.Printf("DeleteKV: deleted customer_id=%v and lookup returned no row\n", row.Values[0])
	return nil
}

func runPrimaryKeyLimitScan(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
		client.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "customer_tier"}
	want := []decodedRow{
		{fields: fields, values: []any{int64(42), "Ada Lovelace", "diamond"}},
		{fields: fields, values: []any{int64(43), "Grace Hopper", "platinum"}},
	}

	limitResult, err := cli.Table(env.kvTable).LimitScan(ctx, nil, 0, int32(len(want)))
	if err != nil {
		return fmt.Errorf("limit scan kv table: %w", err)
	}
	if limitResult.IsLogTable {
		return fmt.Errorf("limit scan kv table: expected primary-key-table result")
	}
	decoded, err := client.DecodeLimitScanRows(schema, limitResult, false)
	if err != nil {
		return fmt.Errorf("decode kv limit scan: %w", err)
	}
	got := make([]decodedRow, 0, len(decoded))
	for _, row := range decoded {
		got = append(got, decodedRow{fields: fields, values: row})
	}
	if !sameDecodedRowSet(got, want) {
		return fmt.Errorf("limit scan kv rows: got %v, want %v", formatDecodedRows(got), formatDecodedRows(want))
	}
	for i, row := range got {
		fmt.Printf("LimitScanKV Row[%d]: %s\n", i, formatRow(row.fields, row.values))
	}
	return nil
}

func runPrimaryKeyPrefixLookup(ctx context.Context, cli *client.Client, env environment) error {
	schema := client.NewSchema(
		client.Int64Type(),
		client.StringType(),
		client.Int64Type(),
		client.StringType(),
	)
	fields := []string{"customer_id", "customer_name", "order_id", "order_status"}
	rows := mustRows(schema, [][]any{
		{int64(1), "aaaaaaaaa", int64(9001), "pending"},
		{int64(1), "aaaaaaaaa", int64(9002), "packed"},
		{int64(2), "aaaaaaaaa", int64(9003), "shipped"},
	})

	for i, row := range rows {
		result, err := cli.Table(env.prefixTable).UpsertRow(ctx, 0, row, nil)
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
		values, err := client.DecodeLookupValuePayload(schema, payload, false)
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

func mustRows(schema client.Schema, values [][]any) []client.Row {
	rows := make([]client.Row, 0, len(values))
	for _, rowValues := range values {
		row, err := client.NewRow(schema, rowValues...)
		if err != nil {
			fatalf("build row: %v", err)
		}
		rows = append(rows, row)
	}
	return rows
}

func mustDecimal(value string, precision, scale int) client.Decimal {
	decimal, err := client.NewDecimalFromString(value, precision, scale)
	if err != nil {
		fatalf("build decimal: %v", err)
	}
	return decimal
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

func samePartitionSpec(got []client.PartitionKV, want client.PartitionSpec) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Key != want[i].Key || got[i].Value != want[i].Value {
			return false
		}
	}
	return true
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

func deepRowsEqual(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !deepValueEqual(got[i], want[i]) {
			return false
		}
	}
	return true
}

func deepValueEqual(got, want any) bool {
	switch wantValue := want.(type) {
	case []byte:
		gotValue, ok := got.([]byte)
		return ok && string(gotValue) == string(wantValue)
	case client.Decimal:
		gotValue, ok := got.(client.Decimal)
		return ok && gotValue.Scale == wantValue.Scale && gotValue.Unscaled.Cmp(wantValue.Unscaled) == 0
	case client.TimestampNtz:
		gotValue, ok := got.(client.TimestampNtz)
		return ok && gotValue == wantValue
	case client.TimestampLtz:
		gotValue, ok := got.(client.TimestampLtz)
		return ok && gotValue == wantValue
	case []any:
		gotValue, ok := got.([]any)
		if !ok || len(gotValue) != len(wantValue) {
			return false
		}
		for i := range wantValue {
			if !deepValueEqual(gotValue[i], wantValue[i]) {
				return false
			}
		}
		return true
	case map[any]any:
		gotValue, ok := got.(map[any]any)
		if !ok || len(gotValue) != len(wantValue) {
			return false
		}
		for key, wantItem := range wantValue {
			gotItem, ok := gotValue[key]
			if !ok || !deepValueEqual(gotItem, wantItem) {
				return false
			}
		}
		return true
	default:
		return got == want
	}
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
