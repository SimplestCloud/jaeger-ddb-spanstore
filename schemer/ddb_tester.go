package schemer

import (
	"context"
	"fmt"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const DDB_DEF_IMAGE = "amazon/dynamodb-local:latest"
const DDB_IMAGE_ENV_NAME = "DDB_IMAGE_TAG"

type DdbConnection struct {
	Conn   *dynamodb.Client
	Config aws.Config
	Ddb    *exec.Cmd
	Port   uint16
}

//noinspection GoUnhandledErrorResult
func (ctx *DdbConnection) Close() {
	ctx.Ddb.Process.Kill()
	ctx.Ddb.Wait()
}

func NewDdbConnection(t *testing.T, failOnErr bool) *DdbConnection {
	// Get a free port
	port, e := utils.GetFreeTcpPort()
	if e != nil {
		t.FailNow()
	}

	image := os.Getenv(DDB_IMAGE_ENV_NAME)
	if image == "" {
		image = DDB_DEF_IMAGE
	}

	// Try to launch the Local DDB that is exposed to localhost
	cmd := exec.Command("docker", "run", "-p", fmt.Sprintf("%d:8000", port),
		"-t", image, "-jar", "DynamoDBLocal.jar", "-inMemory")
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	e = cmd.Start()

	failer := t.SkipNow
	if failOnErr {
		failer = t.FailNow
	}

	if e != nil {
		t.Log("Can't launch the DDB container")
		failer()
	}

	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://localhost:" + strconv.Itoa(port),
			}, nil
		})
	creds := credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID: "AKID", SecretAccessKey: "SECRET", SessionToken: "SESSION",
			Source: "test credentials",
		},
	}

	cfg, e := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(creds), config.WithRegion("mock-region"))
	if e != nil {
		t.Log("Can't launch DDB local: " + e.Error())
		failer()
	}

	// Wait for the database to come online
	conn := dynamodb.NewFromConfig(cfg)
	start := time.Now()
	for {
		_, err := conn.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)

		if time.Now().Sub(start) > 1*time.Minute {
			t.Log("Timed out while waiting for the DB to start")
			failer()
		}
	}

	return &DdbConnection{
		Conn:   conn,
		Config: cfg,
		Ddb:    cmd,
		Port:   uint16(port),
	}
}
