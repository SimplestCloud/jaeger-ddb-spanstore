package spanstore

import (
	"context"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/schemer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var ddbTables = []schemer.Table{
	{
		Name:         "span",
		HashKeyName:  "service_and_time",
		RangeKeyName: "span_id",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
		GSIs: []schemer.GSI{
			{
				Name:            "by-time",
				ProjectionField: "service_and_time",
				RangeKeyField:   "timestamp_nanos",
				RangeKeyType:    types.ScalarAttributeTypeN,
			},
			{
				Name:            "by-duration",
				ProjectionField: "service_and_time",
				RangeKeyField:   "duration_nanos",
				RangeKeyType:    types.ScalarAttributeTypeN,
			},
		},
	},
	{
		Name:         "service",
		HashKeyName:  "name",
		RangeKeyName: "operation",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
	},
	{
		Name:         "dependency",
		HashKeyName:  "parent_service",
		RangeKeyName: "child_service",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
	},
}

func EnsureTablesAreReady(ctx context.Context, config aws.Config) error {
	return nil
}
