package proto

import (
	"fmt"

	flusspb "github.com/chiqors/fluss-go-client/internal/proto/gen/fluss"
	"google.golang.org/protobuf/proto"
)

func New(name string) (proto.Message, error) {
	switch name {
	case "ErrorResponse":
		return &flusspb.ErrorResponse{}, nil
	case "ApiVersionsRequest":
		return &flusspb.ApiVersionsRequest{}, nil
	case "ApiVersionsResponse":
		return &flusspb.ApiVersionsResponse{}, nil
	case "AuthenticateRequest":
		return &flusspb.AuthenticateRequest{}, nil
	case "AuthenticateResponse":
		return &flusspb.AuthenticateResponse{}, nil
	case "ListAclsRequest":
		return &flusspb.ListAclsRequest{}, nil
	case "ListAclsResponse":
		return &flusspb.ListAclsResponse{}, nil
	case "CreateAclsRequest":
		return &flusspb.CreateAclsRequest{}, nil
	case "CreateAclsResponse":
		return &flusspb.CreateAclsResponse{}, nil
	case "DropAclsRequest":
		return &flusspb.DropAclsRequest{}, nil
	case "DropAclsResponse":
		return &flusspb.DropAclsResponse{}, nil
	case "DescribeClusterConfigsRequest":
		return &flusspb.DescribeClusterConfigsRequest{}, nil
	case "DescribeClusterConfigsResponse":
		return &flusspb.DescribeClusterConfigsResponse{}, nil
	case "AlterClusterConfigsRequest":
		return &flusspb.AlterClusterConfigsRequest{}, nil
	case "AlterClusterConfigsResponse":
		return &flusspb.AlterClusterConfigsResponse{}, nil
	case "AddServerTagRequest":
		return &flusspb.AddServerTagRequest{}, nil
	case "AddServerTagResponse":
		return &flusspb.AddServerTagResponse{}, nil
	case "RemoveServerTagRequest":
		return &flusspb.RemoveServerTagRequest{}, nil
	case "RemoveServerTagResponse":
		return &flusspb.RemoveServerTagResponse{}, nil
	case "RebalanceRequest":
		return &flusspb.RebalanceRequest{}, nil
	case "RebalanceResponse":
		return &flusspb.RebalanceResponse{}, nil
	case "ListRebalanceProgressRequest":
		return &flusspb.ListRebalanceProgressRequest{}, nil
	case "ListRebalanceProgressResponse":
		return &flusspb.ListRebalanceProgressResponse{}, nil
	case "CancelRebalanceRequest":
		return &flusspb.CancelRebalanceRequest{}, nil
	case "CancelRebalanceResponse":
		return &flusspb.CancelRebalanceResponse{}, nil
	case "MetadataRequest":
		return &flusspb.MetadataRequest{}, nil
	case "MetadataResponse":
		return &flusspb.MetadataResponse{}, nil
	case "ListDatabasesRequest":
		return &flusspb.ListDatabasesRequest{}, nil
	case "ListDatabasesResponse":
		return &flusspb.ListDatabasesResponse{}, nil
	case "DatabaseExistsRequest":
		return &flusspb.DatabaseExistsRequest{}, nil
	case "DatabaseExistsResponse":
		return &flusspb.DatabaseExistsResponse{}, nil
	case "CreateDatabaseRequest":
		return &flusspb.CreateDatabaseRequest{}, nil
	case "CreateDatabaseResponse":
		return &flusspb.CreateDatabaseResponse{}, nil
	case "DropDatabaseRequest":
		return &flusspb.DropDatabaseRequest{}, nil
	case "DropDatabaseResponse":
		return &flusspb.DropDatabaseResponse{}, nil
	case "GetDatabaseInfoRequest":
		return &flusspb.GetDatabaseInfoRequest{}, nil
	case "GetDatabaseInfoResponse":
		return &flusspb.GetDatabaseInfoResponse{}, nil
	case "ListTablesRequest":
		return &flusspb.ListTablesRequest{}, nil
	case "ListTablesResponse":
		return &flusspb.ListTablesResponse{}, nil
	case "TableExistsRequest":
		return &flusspb.TableExistsRequest{}, nil
	case "TableExistsResponse":
		return &flusspb.TableExistsResponse{}, nil
	case "CreateTableRequest":
		return &flusspb.CreateTableRequest{}, nil
	case "CreateTableResponse":
		return &flusspb.CreateTableResponse{}, nil
	case "AlterTableRequest":
		return &flusspb.AlterTableRequest{}, nil
	case "AlterTableResponse":
		return &flusspb.AlterTableResponse{}, nil
	case "DropTableRequest":
		return &flusspb.DropTableRequest{}, nil
	case "DropTableResponse":
		return &flusspb.DropTableResponse{}, nil
	case "GetTableInfoRequest":
		return &flusspb.GetTableInfoRequest{}, nil
	case "GetTableInfoResponse":
		return &flusspb.GetTableInfoResponse{}, nil
	case "GetTableSchemaRequest":
		return &flusspb.GetTableSchemaRequest{}, nil
	case "GetTableSchemaResponse":
		return &flusspb.GetTableSchemaResponse{}, nil
	case "ListPartitionInfosRequest":
		return &flusspb.ListPartitionInfosRequest{}, nil
	case "ListPartitionInfosResponse":
		return &flusspb.ListPartitionInfosResponse{}, nil
	case "CreatePartitionRequest":
		return &flusspb.CreatePartitionRequest{}, nil
	case "CreatePartitionResponse":
		return &flusspb.CreatePartitionResponse{}, nil
	case "DropPartitionRequest":
		return &flusspb.DropPartitionRequest{}, nil
	case "DropPartitionResponse":
		return &flusspb.DropPartitionResponse{}, nil
	case "ProduceLogRequest":
		return &flusspb.ProduceLogRequest{}, nil
	case "ProduceLogResponse":
		return &flusspb.ProduceLogResponse{}, nil
	case "PutKvRequest":
		return &flusspb.PutKvRequest{}, nil
	case "PutKvResponse":
		return &flusspb.PutKvResponse{}, nil
	case "LookupRequest":
		return &flusspb.LookupRequest{}, nil
	case "LookupResponse":
		return &flusspb.LookupResponse{}, nil
	case "PrefixLookupRequest":
		return &flusspb.PrefixLookupRequest{}, nil
	case "PrefixLookupResponse":
		return &flusspb.PrefixLookupResponse{}, nil
	case "FetchLogRequest":
		return &flusspb.FetchLogRequest{}, nil
	case "FetchLogResponse":
		return &flusspb.FetchLogResponse{}, nil
	case "GetLatestKvSnapshotsRequest":
		return &flusspb.GetLatestKvSnapshotsRequest{}, nil
	case "GetLatestKvSnapshotsResponse":
		return &flusspb.GetLatestKvSnapshotsResponse{}, nil
	case "GetKvSnapshotMetadataRequest":
		return &flusspb.GetKvSnapshotMetadataRequest{}, nil
	case "GetKvSnapshotMetadataResponse":
		return &flusspb.GetKvSnapshotMetadataResponse{}, nil
	case "GetLakeSnapshotRequest":
		return &flusspb.GetLakeSnapshotRequest{}, nil
	case "GetLakeSnapshotResponse":
		return &flusspb.GetLakeSnapshotResponse{}, nil
	case "LimitScanRequest":
		return &flusspb.LimitScanRequest{}, nil
	case "LimitScanResponse":
		return &flusspb.LimitScanResponse{}, nil
	case "ScanKvRequest":
		return &flusspb.ScanKvRequest{}, nil
	case "ScanKvResponse":
		return &flusspb.ScanKvResponse{}, nil
	default:
		return nil, fmt.Errorf("generated proto message not registered: %s", name)
	}
}
