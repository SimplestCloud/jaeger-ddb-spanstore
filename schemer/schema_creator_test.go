package schemer

import (
	"context"
	"github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func TestDbInitializer(t *testing.T) {
	ddb := NewDdbConnection(t, false)
	defer ddb.Close()

	//logger, _ := zap.NewDevelopment()
	ctx := logging.ImbueContext(context.Background(), zap.NewNop())

	schemer := NewDynamoDbInitializer("pre_", "_suffix", ddb.Config)
	tables := []Table{
		{
			Name:         "tokens",
			HashKeyName:  "id",
			RangeKeyName: "range",
			RangeKeyType: ddbtypes.ScalarAttributeTypeS,
			TtlFieldName: "validUntil",
			GSIs: []GSI{{
				Name:            "value-index",
				ProjectionField: "value",
				RangeKeyField:   "range",
				RangeKeyType:    ddbtypes.ScalarAttributeTypeS,
			}},
		},
		{
			Name:        "blobs",
			HashKeyName: "blobId",
		},
	}
	err := schemer.InitSchema(ctx, tables)
	assert.NoError(t, err)

	// InitSchema is idempotent
	err = schemer.InitSchema(ctx, tables)
	require.NoError(t, err)

	// Check a simple DDB request
	values := make(map[string]ddbtypes.AttributeValue)
	values["id"] = &ddbtypes.AttributeValueMemberS{Value: "hello"}
	values["range"] = &ddbtypes.AttributeValueMemberS{Value: "r1"}
	values["value"] = &ddbtypes.AttributeValueMemberS{Value: "world"}

	_, err = ddb.Conn.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("pre_tokens_suffix"),
		Item:      values,
	})
	assert.NoError(t, err)

	resp, err := ddb.Conn.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String("pre_tokens_suffix"),
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"id":    &ddbtypes.AttributeValueMemberS{Value: "hello"},
			"range": &ddbtypes.AttributeValueMemberS{Value: "r1"}},
	})
	require.NoError(t, err)

	assert.Equal(t, "world", resp.Item["value"].(*ddbtypes.AttributeValueMemberS).Value)

	// Check the GSI
	idxResp, err := ddb.Conn.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("pre_tokens_suffix"),
		IndexName: aws.String("value-index"),
	})
	require.NoError(t, err)

	assert.Equal(t, "world", idxResp.Items[0]["value"].(*ddbtypes.AttributeValueMemberS).Value)
	assert.Equal(t, "hello", idxResp.Items[0]["id"].(*ddbtypes.AttributeValueMemberS).Value)
	assert.Equal(t, "r1", idxResp.Items[0]["range"].(*ddbtypes.AttributeValueMemberS).Value)
}
