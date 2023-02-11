package main

import (
	"context"
	"flag"
	. "github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/spanstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

func main() {
	var awsProfile, dbSuffix, listenAddress string
	var debug, create bool
	flag.StringVar(&awsProfile, "aws-profile", "", "AWS profile to use")
	flag.StringVar(&dbSuffix, "db-suffix", "-dev", "DB tables suffix")
	flag.StringVar(&listenAddress, "listen", "[::]:4500", "The network address to listen on")
	flag.BoolVar(&debug, "debug", false, "Debug mode")
	flag.BoolVar(&create, "create-tables", true, "Create missing DynamoDB tables")
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
		err := spanstore.EnsureTablesAreReady(ctx, awsConfig)
		if err != nil {
			L(ctx).Fatal("Failed to create tables", zap.Error(err))
		}
	}

	plug := spanstore.Plugin{}

	L(ctx).Info("Opening listener", zap.String("listen-address", listenAddress))

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		panic(err)
	}

	L(ctx).Info("Starting the GRPC server")

	server := grpc.NewServer()
	plugins := shared.NewGRPCHandlerWithPlugins(&plug, &plug, &plug)
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
