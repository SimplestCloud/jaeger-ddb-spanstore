package utils

import (
	"context"
	"github.com/Cyberax/argus-vision/visibility/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type customContextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *customContextStream) Context() context.Context {
	return s.ctx
}

type LoggingInterceptors struct {
	logger *zap.Logger
}

// NewLoggingInterceptors Creates a new GRPC interceptor that attaches a logger to GRPC requests
func NewLoggingInterceptors(logger *zap.Logger) *LoggingInterceptors {
	return &LoggingInterceptors{
		logger: logger,
	}
}

func (g *LoggingInterceptors) UnaryInterceptor(ctx context.Context, req interface{},
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	ctx = logging.ImbueContext(ctx, g.logger)

	return handler(ctx, req)
}

func (g *LoggingInterceptors) StreamInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	ctx := logging.ImbueContext(ss.Context(), g.logger)

	return handler(srv, &customContextStream{ss, ctx})
}
