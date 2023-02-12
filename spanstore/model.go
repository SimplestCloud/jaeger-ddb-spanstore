package spanstore

import (
	"context"
	"encoding/base64"
	"fmt"
	. "github.com/Cyberax/argus-vision/visibility/logging"
	"github.com/SimplestCloud/jaeger-ddb-spanstore/schemer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jaegertracing/jaeger/model"
	"time"
)

const SpanTableName = "span"

var ddbTables = []schemer.Table{
	{
		Name:         SpanTableName,
		HashKeyName:  "service_and_time",
		RangeKeyName: "span_id",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
		GSIs: []schemer.GSI{
			{
				Name:            "by-time",
				ProjectionField: "service_and_time",
				RangeKeyField:   "start_time_nanos",
				RangeKeyType:    types.ScalarAttributeTypeN,
			},
			{
				Name:            "by-duration",
				ProjectionField: "service_and_time",
				RangeKeyField:   "duration_nanos",
				RangeKeyType:    types.ScalarAttributeTypeN,
			},
		},
	},
	{
		Name:         "service",
		HashKeyName:  "name",
		RangeKeyName: "operation",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
	},
	{
		Name:         "dependency",
		HashKeyName:  "parent_service",
		RangeKeyName: "child_service",
		RangeKeyType: types.ScalarAttributeTypeS,
		TtlFieldName: "ttl",
	},
}

func EnsureTablesAreReady(ctx context.Context, suffix string, config aws.Config) error {
	initializer := schemer.NewDynamoDbInitializer("", suffix, config)

	L(ctx).Info("Ensuring tables are present")
	err := initializer.InitSchema(ctx, ddbTables)
	if err != nil {
		return err
	}
	L(ctx).Info("Tables are ready")
	return nil
}

// StoredSpanRef the stored version of model.SpanRef
type StoredSpanRef struct {
	TraceId string            `dynamodbav:"trace_id,omitempty"`
	SpanId  string            `dynamodbav:"span_id,omitempty"`
	RefType model.SpanRefType `dynamodbav:"ref_type,omitempty"`
}

// StoredKeyValue the stored version of model.KeyValue
type StoredKeyValue struct {
	Key      string          `dynamodbav:"key,omitempty"`
	VType    model.ValueType `dynamodbav:"value_type,omitempty"`
	VStr     string          `dynamodbav:"v_str,omitempty"`
	VBool    bool            `dynamodbav:"v_bool,omitempty"`
	VInt64   int64           `dynamodbav:"v_int64,omitempty"`
	VFloat64 float64         `dynamodbav:"v_float64,omitempty"`
	VBinary  []byte          `dynamodbav:"v_binary,omitempty"`
}

// StoredLog the stored version of model.Log
type StoredLog struct {
	Timestamp int64            `dynamodbav:"timestamp_nanos,omitempty"`
	Fields    []StoredKeyValue `dynamodbav:"fields,omitempty"`
}

// StoredProcess the stored version of model.Process
type StoredProcess struct {
	ServiceName string           `dynamodbav:"service_name,omitempty"`
	Tags        []StoredKeyValue `dynamodbav:"tags,omitempty"`
}

// StoredSpan the stored version of model.Span
type StoredSpan struct {
	// The bucket key
	ServiceAndTime string            `dynamodbav:"service_and_time,omitempty"`
	FlattenedTags  map[string]string `dynamodbav:"flattened_tags,omitempty"`

	TraceId       string           `dynamodbav:"trace_id,omitempty"`
	SpanId        string           `dynamodbav:"span_id,omitempty"`
	OperationName string           `dynamodbav:"operation_name,omitempty"`
	References    []StoredSpanRef  `dynamodbav:"references,omitempty"`
	Flags         model.Flags      `dynamodbav:"flags,omitempty"`
	StartTime     int64            `dynamodbav:"start_time_nanos,omitempty"`
	Duration      time.Duration    `dynamodbav:"duration_nanos,omitempty"`
	Tags          []StoredKeyValue `dynamodbav:"tags,omitempty"`
	Logs          []StoredLog      `dynamodbav:"logs,omitempty"`
	Process       *StoredProcess   `dynamodbav:"process,omitempty"`
	ProcessId     string           `dynamodbav:"process_id,omitempty"`
	Warnings      []string         `dynamodbav:"warnings,omitempty"`
}

func ToDdbModel(span *model.Span) (*StoredSpan, error) {
	res := &StoredSpan{}

	// We bucket by every hour to ensure sharing
	timeBucket := span.StartTime.UTC().Format("2006-01-02-15")
	res.ServiceAndTime = res.Process.ServiceName + "-" + timeBucket

	res.TraceId = formatTraceId(span.TraceID)
	res.SpanId = fmt.Sprintf("%x", span.SpanID)
	res.OperationName = span.OperationName
	res.References = translateReferences(span.References)
	res.Flags = span.Flags
	res.StartTime = span.StartTime.UnixNano()
	res.Duration = span.Duration
	res.Tags = translateTags(span.Tags)
	res.Logs = translateLogs(span.Logs)
	res.Process = translateProcess(span.Process)
	res.ProcessId = span.ProcessID
	res.Warnings = span.Warnings

	res.FlattenedTags = map[string]string{}
	flattenTags(span.Tags, res.FlattenedTags)
	flattenTags(span.Process.Tags, res.FlattenedTags)

	return res, nil
}

func formatTraceId(tid model.TraceID) string {
	return fmt.Sprintf("%x%x", tid.High, tid.Low)
}

func translateProcess(process *model.Process) *StoredProcess {
	return &StoredProcess{
		ServiceName: process.ServiceName,
		Tags:        translateTags(process.Tags),
	}
}

func translateReferences(references []model.SpanRef) []StoredSpanRef {
	var res []StoredSpanRef
	for _, r := range references {
		res = append(res, StoredSpanRef{
			TraceId: formatTraceId(r.TraceID),
			SpanId:  fmt.Sprintf("%x", r.SpanID),
			RefType: r.RefType,
		})
	}
	return res
}

func translateLogs(logs []model.Log) []StoredLog {
	var res []StoredLog
	for _, l := range logs {
		res = append(res, StoredLog{
			Timestamp: l.Timestamp.UnixNano(),
			Fields:    translateTags(l.Fields),
		})
	}
	return res
}

func flattenTags(tags []model.KeyValue, res map[string]string) {
	for _, t := range tags {
		switch t.VType {
		case model.ValueType_STRING:
			res[t.Key] = t.VStr
		case model.ValueType_BOOL:
			res[t.Key] = fmt.Sprintf("%t", t.VBool)
		case model.ValueType_INT64:
			res[t.Key] = fmt.Sprintf("%d", t.VInt64)
		case model.ValueType_FLOAT64:
			res[t.Key] = fmt.Sprintf("%f", t.VFloat64)
		case model.ValueType_BINARY:
			res[t.Key] = base64.StdEncoding.EncodeToString(t.VBinary)
		}
	}
}

func translateTags(tags []model.KeyValue) []StoredKeyValue {
	var res []StoredKeyValue
	for _, t := range tags {
		res = append(res, StoredKeyValue{
			Key:      t.Key,
			VType:    t.VType,
			VStr:     t.VStr,
			VBool:    t.VBool,
			VInt64:   t.VInt64,
			VFloat64: t.VFloat64,
			VBinary:  t.VBinary,
		})
	}
	return res
}
