package spanstore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type DdbWriter struct {
	client *dynamodb.Client
	suffix string
}

var _ spanstore.Writer = &DdbWriter{}

func NewDdbWriter(client *dynamodb.Client, suffix string) *DdbWriter {
	return &DdbWriter{
		client: client,
		suffix: suffix,
	}
}

func (d *DdbWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	ddbModel, err := ToDdbModel(span)
	if err != nil {
		return fmt.Errorf("failed to convert to DDB model: %w", err)
	}

	ddbModelMap, err := attributevalue.MarshalMap(ddbModel)
	if err != nil {
		return err
	}

	// Add the dependency links

	// Save the span
	_, err = d.client.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      ddbModelMap,
		TableName: aws.String(SpanTableName + d.suffix),
	})
	// TODO: gracefully handle too large items
	if err != nil {
		return fmt.Errorf("failed to persist the span: %w", err)
	}

	return nil
}
