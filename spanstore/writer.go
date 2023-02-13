package spanstore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"time"
)

type DdbWriter struct {
	client *dynamodb.Client
	suffix string
	dep    *DependencyManager

	ttlSeconds int64
	timer      func() time.Time
}

var _ spanstore.Writer = &DdbWriter{}

func NewDdbWriter(client *dynamodb.Client, suffix string, ttlSeconds int64,
	dep *DependencyManager) *DdbWriter {

	return &DdbWriter{
		client:     client,
		suffix:     suffix,
		ttlSeconds: ttlSeconds,
		dep:        dep,
		timer:      time.Now,
	}
}

func (d *DdbWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	serviceName := span.Process.ServiceName
	operationName := span.OperationName

	ddbModel, err := ToDdbModel(span)
	if err != nil {
		return fmt.Errorf("failed to convert to DDB model: %w", err)
	}

	ddbModelMap, err := attributevalue.MarshalMap(ddbModel)
	if err != nil {
		return err
	}

	// Set the record TTL
	ddbModelMap["ttl"] = &types.AttributeValueMemberN{
		Value: fmt.Sprintf("%d", time.Now().Unix()+d.ttlSeconds)}

	// Add the dependency links
	err = d.dep.RegisterCall(ctx, serviceName, operationName, ddbModel.TraceId, ddbModel.SpanId)
	if err != nil {
		return fmt.Errorf("failed to register a service call: %w", err)
	}

	// Process dependencies so that we can rebuild the call chain
	for _, r := range span.References {
		err = d.dep.RegisterReference(ctx, serviceName, operationName,
			formatTraceId(r.TraceID), formatSpanId(r.SpanID))
		if err != nil {
			return fmt.Errorf("failed to register a dependency: %w", err)
		}
	}

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
