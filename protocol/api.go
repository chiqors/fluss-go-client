package protocol

type APIKey int16

const (
	APIVersions        APIKey = 1000
	CreateDatabase     APIKey = 1001
	DropDatabase       APIKey = 1002
	ListDatabases      APIKey = 1003
	DatabaseExists     APIKey = 1004
	CreateTable        APIKey = 1005
	DropTable          APIKey = 1006
	GetTableInfo       APIKey = 1007
	ListTables         APIKey = 1008
	ListPartitionInfos APIKey = 1009
	TableExists        APIKey = 1010
	GetTableSchema     APIKey = 1011
	GetMetadata        APIKey = 1012
	ProduceLog         APIKey = 1014
	FetchLog           APIKey = 1015
	PutKV              APIKey = 1016
	Lookup             APIKey = 1017
	ListOffsets        APIKey = 1021
	InitWriter         APIKey = 1026
	LimitScan          APIKey = 1033
	PrefixLookup       APIKey = 1034
	GetDatabaseInfo    APIKey = 1035
	Authenticate       APIKey = 1038
	ScanKV             APIKey = 1061
)

type ResponseType byte

const (
	ResponseSuccess ResponseType = 0
	ResponseError   ResponseType = 1
	ResponseFailure ResponseType = 2
)
