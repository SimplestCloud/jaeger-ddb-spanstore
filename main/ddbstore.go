package main

import (
	"context"
	"flag"
	. "github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/spanstore"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"go.uber.org/zap/zapcore"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

func main() {
	var awsProfile, dbSuffix, listenAddress string
	var debug, create bool
	var ttlDays, archiveTtlDays int64
	flag.StringVar(&awsProfile, "aws-profile", "", "AWS profile to use")
	flag.StringVar(&dbSuffix, "db-suffix", "-dev", "DB tables suffix")
	flag.StringVar(&listenAddress, "listen", "[::]:4500", "The network address to listen on")
	flag.BoolVar(&debug, "debug", false, "Debug mode")
	flag.BoolVar(&create, "create-tables", true, "Create missing DynamoDB tables")
	flag.Int64Var(&ttlDays, "ttl-days", 60, "TTL for traces (in days)")
	flag.Int64Var(&archiveTtlDays, "archive-ttl-days", 180, "TTL for archived traces (in days)")
	flag.Parse()

	var ctx context.Context
	if debug {
		logger := ConfigureDevLogger()
		ctx = ImbueContext(context.Background(), logger)
	} else {
		logger := ConfigureProdLogger()
		ctx = ImbueContext(context.Background(), logger)
	}

	awsConfig := prepareAws(ctx, awsProfile)

	if create {
		err := spanstore.EnsureTablesAreReady(ctx, dbSuffix, awsConfig)
		if err != nil {
			L(ctx).Fatal("Failed to create tables", zap.Error(err))
		}
	}

	dbClient := dynamodb.NewFromConfig(awsConfig)

	depManager := spanstore.NewDependencyManager(dbClient, dbSuffix, archiveTtlDays*86400)
	depManager.Start()
	defer depManager.Stop()

	reader := &spanstore.DdbReader{}

	writer := spanstore.NewDdbWriter(dbClient, dbSuffix, ttlDays*86400, depManager)
	archiveWriter := spanstore.NewDdbWriter(dbClient, dbSuffix, archiveTtlDays*86400, depManager)

	plug := spanstore.NewPlugin(reader, writer, archiveWriter)

	L(ctx).Info("Opening listener", zap.String("listen-address", listenAddress))

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		panic(err)
	}

	L(ctx).Info("Starting the GRPC server")

	// Create the interceptor that will attach the logger to the GRPC context
	logInt := utils.NewLoggingInterceptors(L(ctx))

	opts := []grpc_zap.Option{
		grpc_zap.WithDurationField(func(duration time.Duration) zapcore.Field {
			return zap.Int64("grpc.time_us", duration.Microseconds())
		}),
	}

	// Create the GRPC server and start it
	server := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			logInt.UnaryInterceptor,
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(L(ctx), opts...),
		),
		grpc_middleware.WithStreamServerChain(
			logInt.StreamInterceptor,
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(L(ctx), opts...),
		))

	plugins := shared.NewGRPCHandlerWithPlugins(plug, plug, plug)
	_ = plugins.Register(server)

	_ = server.Serve(listener)
}

func prepareAws(ctx context.Context, profile string) aws.Config {
	var options []func(options *config.LoadOptions) error

	if profile != "" {
		options = append(options, config.WithSharedConfigProfile(profile))
	}

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		L(ctx).Fatal("Failed to load AWS config", zap.Error(err))
	}

	return cfg
}
