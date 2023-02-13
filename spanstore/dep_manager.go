package spanstore

import (
	"context"
	"fmt"
	. "github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"net/url"
	"sync"
	"time"
)

type callTarget struct {
	operationName, serviceName string
}

type DependencyManager struct {
	client *dynamodb.Client
	suffix string

	ttlSeconds int64
	timer      func() time.Time

	mtx          sync.Mutex
	serviceCache map[string]time.Time

	callCache *ttlcache.Cache[string, callTarget]
}

func NewDependencyManager(client *dynamodb.Client, suffix string, ttlSeconds int64) *DependencyManager {
	return &DependencyManager{
		client:       client,
		suffix:       suffix,
		ttlSeconds:   ttlSeconds,
		timer:        time.Now,
		serviceCache: make(map[string]time.Time),
		callCache:    ttlcache.New[string, callTarget](),
	}
}

func (d *DependencyManager) Start() {
	d.callCache.Start()
}

func (d *DependencyManager) Stop() {
	d.callCache.Stop()
}

func (d *DependencyManager) RegisterCall(ctx context.Context, service, operation,
	traceId, spanId string) error {
	d.cacheCallId(traceId, spanId, service, operation)

	if d.checkCache(service, operation) {
		L(ctx).Debug("We've seen this operation before", zap.String("service", service),
			zap.String("operation", operation))
		return nil
	}

	L(ctx).Info("Recording a service and operation", zap.String("service", service),
		zap.String("operation", operation))

	ss := &StoredService{
		Service:   service,
		Operation: operation,
	}

	ddbModelMap, err := attributevalue.MarshalMap(ss)
	if err != nil {
		return err
	}

	// Set the record TTL
	ddbModelMap["ttl"] = &types.AttributeValueMemberN{
		Value: fmt.Sprintf("%d", time.Now().Unix()+d.ttlSeconds)}

	_, err = d.client.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      ddbModelMap,
		TableName: aws.String(ServiceTableName + d.suffix),
	})
	if err != nil {
		return fmt.Errorf("failed to record the operation: %w", err)
	}

	// Make a note that we recorded the operation
	d.mtx.Lock()
	defer d.mtx.Unlock()
	cacheKey := url.QueryEscape(service) + "#" + url.QueryEscape(operation)
	d.serviceCache[cacheKey] = d.timer()

	return nil
}

func (d *DependencyManager) checkCache(service string, operation string) bool {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	cacheKey := url.QueryEscape(service) + "#" + url.QueryEscape(operation)
	lastSaved, ok := d.serviceCache[cacheKey]
	if !ok || lastSaved.After(d.timer().Add(time.Duration(d.ttlSeconds)*time.Second/10)) {
		return false // Need to re-save the operation
	}

	// We're in cache!
	return true
}

func (d *DependencyManager) cacheCallId(traceId string, spanId string,
	service string, operation string) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.callCache.Set(traceId+"#"+spanId, callTarget{
		operationName: operation,
		serviceName:   service,
	}, 86400*time.Second)
}

func (d *DependencyManager) RegisterReference(ctx context.Context, childService, childOperation,
	parentTraceId, parentSpanId string) error {

	return nil
}
