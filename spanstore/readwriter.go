package spanstore

import (
	"context"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"time"
)

type ReadWriter struct {
}

var _ spanstore.Reader = &ReadWriter{}
var _ dependencystore.Reader = &ReadWriter{}

func (r ReadWriter) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	//TODO implement me
	panic("implement me")
}

func (r ReadWriter) GetServices(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (r ReadWriter) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	//TODO implement me
	panic("implement me")
}

func (r ReadWriter) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	//TODO implement me
	panic("implement me")
}

func (r ReadWriter) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	//TODO implement me
	panic("implement me")
}

func (r ReadWriter) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	//TODO implement me
	panic("implement me")
}
