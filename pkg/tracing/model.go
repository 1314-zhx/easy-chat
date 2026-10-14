package tracing

import (
	"time"
)

// SpanKind represents the kind of span (Jaeger format)
type SpanKind string

const (
	SpanKindUnspecified SpanKind = "SPAN_KIND_UNSPECIFIED"
	SpanKindServer      SpanKind = "SERVER"
	SpanKindClient      SpanKind = "CLIENT"
	SpanKindProducer    SpanKind = "PRODUCER"
	SpanKindConsumer    SpanKind = "CONSUMER"
)

// KeyValue represents a key-value tag
type KeyValue struct {
	Key   string `json:"key" bson:"key"`
	Type  string `json:"type,omitempty" bson:"type,omitempty"` // string, int, bool, float64
	Value any    `json:"value" bson:"value"`
}

// Log represents a log entry in a span
type Log struct {
	Timestamp time.Time  `json:"timestamp" bson:"timestamp"`
	Fields    []KeyValue `json:"fields,omitempty" bson:"fields,omitempty"`
}

// SpanReference represents a reference to another span
type SpanReference struct {
	TraceID string `json:"traceId" bson:"trace_id"`
	SpanID  string `json:"spanId" bson:"span_id"`
	RefType string `json:"refType" bson:"ref_type"` // CHILD_OF or FOLLOWS_FROM
}

// TraceSpan represents a single span in a trace (Jaeger format)
type TraceSpan struct {
	TraceID       string           `json:"traceId" bson:"trace_id"`
	SpanID        string           `json:"spanId" bson:"span_id"`
	ParentSpanID  string           `json:"parentSpanId,omitempty" bson:"parent_span_id,omitempty"`
	OperationName string           `json:"operationName" bson:"operation_name"`
	ServiceName   string           `json:"serviceName" bson:"service_name"`
	Kind          SpanKind         `json:"kind" bson:"kind"`
	StartTime     time.Time        `json:"startTime" bson:"start_time"`
	Duration      int64            `json:"duration" bson:"duration"` // 微秒
	Tags          []KeyValue       `json:"tags,omitempty" bson:"tags,omitempty"`
	Logs          []Log            `json:"logs,omitempty" bson:"logs,omitempty"`
	References    []SpanReference  `json:"references,omitempty" bson:"references,omitempty"`
	StatusCode    int              `json:"statusCode,omitempty" bson:"status_code,omitempty"`
	Flag          int              `json:"flag,omitempty" bson:"flag,omitempty"`
	CreatedAt     time.Time        `json:"createdAt,omitempty" bson:"created_at,omitempty"`
}

// Process represents service information
type Process struct {
	ServiceName string     `json:"serviceName" bson:"service_name"`
	Tags        []KeyValue `json:"tags,omitempty" bson:"tags,omitempty"`
}

// Trace represents a complete trace
type Trace struct {
	TraceID  string       `json:"traceId" bson:"trace_id"`
	Spans    []TraceSpan  `json:"spans" bson:"spans"`
	Process  Process     `json:"process,omitempty" bson:"process,omitempty"`
	Warnings []string    `json:"warnings,omitempty" bson:"warnings,omitempty"`
}
