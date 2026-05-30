package metadata

import "encoding/json"

type tableDescriptorJSON struct {
	Schema struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		PrimaryKey []string `json:"primary_key"`
	} `json:"schema"`
	PartitionKey []string `json:"partition_key"`
	BucketKey    []string `json:"bucket_key"`
}

func ParseTableDescriptor(tableJSON []byte) (columnNames []string, partitionKeys []string, primaryKeys []string, bucketKeys []string, err error) {
	var desc tableDescriptorJSON
	if err := json.Unmarshal(tableJSON, &desc); err != nil {
		return nil, nil, nil, nil, err
	}
	columnNames = make([]string, 0, len(desc.Schema.Columns))
	for _, column := range desc.Schema.Columns {
		columnNames = append(columnNames, column.Name)
	}
	partitionKeys = append([]string(nil), desc.PartitionKey...)
	primaryKeys = append([]string(nil), desc.Schema.PrimaryKey...)
	bucketKeys = append([]string(nil), desc.BucketKey...)
	return columnNames, partitionKeys, primaryKeys, bucketKeys, nil
}

func ParseTableKeys(tableJSON []byte) (primaryKeys []string, bucketKeys []string, err error) {
	_, _, primaryKeys, bucketKeys, err = ParseTableDescriptor(tableJSON)
	return primaryKeys, bucketKeys, err
}
