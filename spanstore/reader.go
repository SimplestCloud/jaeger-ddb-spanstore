package spanstore

import (
	"context"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"time"
)

type DdbReader struct {
}

var _ spanstore.Reader = &DdbReader{}
var _ dependencystore.Reader = &DdbReader{}

func (r DdbReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	//TODO implement me
	panic("implement me")
}

func (r DdbReader) GetServices(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (r DdbReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	//TODO implement me
	panic("implement me")
}

func (r DdbReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	//TODO implement me
	panic("implement me")
}

func (r DdbReader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	//TODO implement me
	panic("implement me")
}

func (r DdbReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	//TODO implement me
	panic("implement me")
}
