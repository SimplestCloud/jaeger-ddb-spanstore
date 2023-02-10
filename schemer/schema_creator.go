package schemer

import (
	"context"
	. "github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/zap"
	"strings"
	"time"
)

type DynamoDBInitializer struct {
	Prefix    string
	Suffix    string
	AwsConfig aws.Config
}

func NewDynamoDbInitializer(prefix, suffix string, config aws.Config) *DynamoDBInitializer {
	return &DynamoDBInitializer{
		Prefix:    prefix,
		Suffix:    suffix,
		AwsConfig: config,
	}
}

type GSI struct {
	Name            string
	ProjectionField string

	RangeKeyField string
	RangeKeyType  ddbtypes.ScalarAttributeType
}

type Table struct {
	Name        string
	HashKeyName string

	RangeKeyName string
	RangeKeyType ddbtypes.ScalarAttributeType

	TtlFieldName string

	GSIs []GSI
}

func (db *DynamoDBInitializer) decorateTableName(name string) string {
	return db.Prefix + name + db.Suffix
}

func (db *DynamoDBInitializer) trimTableName(name string) string {
	return strings.TrimPrefix(strings.TrimSuffix(name, db.Suffix), db.Prefix)
}

func (db *DynamoDBInitializer) InitSchema(ctx context.Context, tablesToCreate []Table) error {
	L(ctx).Info("Describing tables")

	svc := dynamodb.NewFromConfig(db.AwsConfig)

	var tables = make(map[string]int64)
	lti := dynamodb.ListTablesInput{}
	for {
		output, err := svc.ListTables(ctx, &lti)
		if err != nil {
			return err
		}

		for _, t := range output.TableNames {
			tables[db.trimTableName(t)] = 1
		}

		if output.LastEvaluatedTableName == nil {
			break
		}
		lti.ExclusiveStartTableName = output.LastEvaluatedTableName
	}

	// Now create the missing tables
	for _, t := range tablesToCreate {
		if _, ok := tables[t.Name]; ok {
			L(ctx).Info("Table exists", zap.String("table-name", t.Name))

			err := db.ensureGsi(ctx, svc, db.decorateTableName(t.Name), t.GSIs)
			if err != nil {
				return err
			}
			err = db.ensureTtlIsSet(ctx, svc, db.decorateTableName(t.Name), t.TtlFieldName)
			if err != nil {
				return err
			}

			continue
		}

		newTableName := db.decorateTableName(t.Name)

		L(ctx).Info("Creating table", zap.String("table-name", newTableName))

		attrDefs := []ddbtypes.AttributeDefinition{{
			AttributeName: aws.String(t.HashKeyName), AttributeType: "S"},
		}
		keySchema := []ddbtypes.KeySchemaElement{{
			AttributeName: aws.String(t.HashKeyName), KeyType: "HASH",
		}}

		if t.RangeKeyName != "" {
			attrDefs = append(attrDefs, ddbtypes.AttributeDefinition{
				AttributeName: aws.String(t.RangeKeyName), AttributeType: t.RangeKeyType})
			keySchema = append(keySchema, ddbtypes.KeySchemaElement{
				AttributeName: aws.String(t.RangeKeyName), KeyType: "RANGE",
			})
		}

		_, err := svc.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:            aws.String(newTableName),
			AttributeDefinitions: attrDefs,
			KeySchema:            keySchema,
			BillingMode:          ddbtypes.BillingModePayPerRequest,
		})

		if err != nil {
			return err
		}

		waiter := dynamodb.NewTableExistsWaiter(svc)
		params := &dynamodb.DescribeTableInput{TableName: aws.String(newTableName)}
		err = waiter.Wait(ctx, params, 5*time.Minute)
		if err != nil {
			return err
		}

		err = db.ensureGsi(ctx, svc, newTableName, t.GSIs)
		if err != nil {
			return err
		}

		err = db.ensureTtlIsSet(ctx, svc, newTableName, t.TtlFieldName)
		if err != nil {
			return err
		}
	}

	L(ctx).Info("All tables are ready")
	return nil
}

func (db *DynamoDBInitializer) ensureTtlIsSet(ctx context.Context,
	client *dynamodb.Client, tableName string, ttlField string) error {

	if ttlField == "" {
		return nil
	}

	ctx = WithFields(ctx, zap.String("table-name", tableName),
		zap.String("ttl-field", ttlField))

	response, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName)})
	if err != nil {
		return err
	}

	if response.TimeToLiveDescription == nil ||
		response.TimeToLiveDescription.TimeToLiveStatus == ddbtypes.TimeToLiveStatusDisabled {

		L(ctx).Info("Creating the TTL field")
		_, err := client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(tableName),
			TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
				AttributeName: aws.String(ttlField),
				Enabled:       aws.Bool(true),
			},
		})
		if err != nil {
			return err
		}
		L(ctx).Info("Finished creating the TTL field")
	}

	L(ctx).Info("TTL field is up-to-date")

	return nil
}

func (db *DynamoDBInitializer) ensureGsi(ctx context.Context, client *dynamodb.Client,
	tableName string, gsis []GSI) error {

	if len(gsis) == 0 {
		return nil
	}

	ctx = WithFields(ctx, zap.String("table-name", tableName))

	L(ctx).Info("Checking the table's GSIs")

	response, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	existing := map[string]bool{}
	for _, i := range response.Table.GlobalSecondaryIndexes {
		existing[*i.IndexName] = true
	}

	var newGsis []ddbtypes.GlobalSecondaryIndexUpdate
	var attrDefs []ddbtypes.AttributeDefinition
	for _, gsi := range gsis {
		if existing[gsi.Name] {
			L(ctx).Info("GSI exists", zap.String("gsi-name", gsi.Name))
			continue
		}

		L(ctx).Info("GSI will be created", zap.String("gsi-name", gsi.Name))
		keySchemaElems := []ddbtypes.KeySchemaElement{{
			AttributeName: aws.String(gsi.ProjectionField),
			KeyType:       ddbtypes.KeyTypeHash,
		}}
		attrDefs = append(attrDefs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String(gsi.ProjectionField), AttributeType: "S"})

		if gsi.RangeKeyField != "" {
			keySchemaElems = append(keySchemaElems, ddbtypes.KeySchemaElement{
				AttributeName: aws.String(gsi.RangeKeyField),
				KeyType:       ddbtypes.KeyTypeRange,
			})
			attrDefs = append(attrDefs, ddbtypes.AttributeDefinition{
				AttributeName: aws.String(gsi.RangeKeyField), AttributeType: gsi.RangeKeyType})
		}
		newGsis = append(newGsis, ddbtypes.GlobalSecondaryIndexUpdate{
			Create: &ddbtypes.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String(gsi.Name),
				KeySchema: keySchemaElems,
				Projection: &ddbtypes.Projection{
					ProjectionType: ddbtypes.ProjectionTypeAll,
				},
				ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(0),
					WriteCapacityUnits: aws.Int64(0),
				},
			},
		})
	}

	if len(newGsis) != 0 {
		L(ctx).Info("Creating the missing GSIs")

		_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:                   aws.String(tableName),
			GlobalSecondaryIndexUpdates: newGsis,
			AttributeDefinitions:        attrDefs,
		})
		if err != nil {
			return err
		}
	}

	// We wait for GSIs to become active, even if nothing new was created, on an
	// off-chance we were restarted during waiting.
	err = db.waitForGsi(ctx, tableName, client)
	if err != nil {
		return err
	}

	L(ctx).Info("GSI are ready", zap.String("table-name", tableName))
	return nil
}

func (db *DynamoDBInitializer) waitForGsi(ctx context.Context, tableName string,
	client *dynamodb.Client) error {

	indexesAreReady := func(ctx context.Context, input *dynamodb.DescribeTableInput,
		output *dynamodb.DescribeTableOutput, err error) (bool, error) {

		// We stop at the first error
		if err != nil {
			return false, err
		}

		for _, i := range output.Table.GlobalSecondaryIndexes {
			if i.IndexStatus == ddbtypes.IndexStatusCreating {
				return true, nil
			}
		}

		// All indexes are created!
		return false, nil
	}

	waiter := dynamodb.NewTableExistsWaiter(client, func(options *dynamodb.TableExistsWaiterOptions) {
		options.MinDelay = 100 * time.Millisecond
		options.Retryable = indexesAreReady
	})

	params := &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}
	err := waiter.Wait(ctx, params, 5*time.Minute)
	if err != nil {
		return err
	}

	return nil
}
