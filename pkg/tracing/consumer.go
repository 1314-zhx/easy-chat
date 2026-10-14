package tracing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoTraceStorage MongoDB 存储
type MongoTraceStorage struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
}

// NewMongoTraceStorage 创建 MongoDB 存储
func NewMongoTraceStorage(ctx context.Context, uri, dbName string) (*MongoTraceStorage, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("连接MongoDB失败: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("Ping MongoDB失败: %w", err)
	}

	db := client.Database(dbName)
	collection := db.Collection("traces")

	// 创建索引
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "trace_id", Value: 1}}},
		{Keys: bson.D{{Key: "service_name", Value: 1}}},
		{Keys: bson.D{{Key: "operation_name", Value: 1}}},
		{Keys: bson.D{{Key: "start_time", Value: -1}}},
		{Keys: bson.D{
			{Key: "service_name", Value: 1},
			{Key: "operation_name", Value: 1},
			{Key: "start_time", Value: -1},
		}},
	}

	_, err = collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		logx.Errorf("创建trace索引失败: %v", err)
	}

	logx.Info("MongoDB trace storage 初始化成功")
	return &MongoTraceStorage{
		client:     client,
		database:   db,
		collection: collection,
	}, nil
}

// StoreBatch 批量存储 spans
func (s *MongoTraceStorage) StoreBatch(ctx context.Context, spans []TraceSpan) error {
	if len(spans) == 0 {
		return nil
	}

	docs := make([]interface{}, len(spans))
	now := time.Now()
	for i := range spans {
		spans[i].CreatedAt = now
		docs[i] = spans[i]
	}

	_, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return fmt.Errorf("插入spans失败: %w", err)
	}

	logx.Infof("成功存储 %d 个spans", len(spans))
	return nil
}

// QueryTraces 查询 traces
func (s *MongoTraceStorage) QueryTraces(ctx context.Context, serviceName, operationName string, limit int64) ([]Trace, error) {
	filter := bson.M{}
	if serviceName != "" {
		filter["service_name"] = serviceName
	}
	if operationName != "" {
		filter["operation_name"] = operationName
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "start_time", Value: -1}}).
		SetLimit(limit)

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// 收集 trace IDs
	traceIDSet := make(map[string]bool)
	for cursor.Next(ctx) {
		var span TraceSpan
		if err := cursor.Decode(&span); err != nil {
			continue
		}
		traceIDSet[span.TraceID] = true
	}

	// 获取所有 spans
	traceIDs := make([]string, 0, len(traceIDSet))
	for id := range traceIDSet {
		traceIDs = append(traceIDs, id)
	}

	if len(traceIDs) == 0 {
		return nil, nil
	}

	filter = bson.M{"trace_id": bson.M{"$in": traceIDs}}
	opts = options.Find().SetSort(bson.D{{Key: "start_time", Value: 1}})
	cursor, err = s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// 按 trace ID 分组
	traceMap := make(map[string][]TraceSpan)
	serviceMap := make(map[string]string)
	for cursor.Next(ctx) {
		var span TraceSpan
		if err := cursor.Decode(&span); err != nil {
			continue
		}
		traceMap[span.TraceID] = append(traceMap[span.TraceID], span)
		if span.ServiceName != "" {
			serviceMap[span.TraceID] = span.ServiceName
		}
	}

	// 构建 Trace 列表
	traces := make([]Trace, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		spans, ok := traceMap[traceID]
		if !ok || len(spans) == 0 {
			continue
		}
		traces = append(traces, Trace{
			TraceID:  traceID,
			Spans:    spans,
			Process:  Process{ServiceName: serviceMap[traceID]},
		})
	}

	return traces, nil
}

// Close 关闭连接
func (s *MongoTraceStorage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// =====================================================

// TraceHandler Kafka 消费者，实现 kq.Pusher 接口
type TraceHandler struct {
	storage *MongoTraceStorage
}

// NewTraceHandler 创建 handler
func NewTraceHandler(ctx context.Context, mongoUri, mongoDb string) (*TraceHandler, error) {
	storage, err := NewMongoTraceStorage(ctx, mongoUri, mongoDb)
	if err != nil {
		return nil, err
	}
	return &TraceHandler{storage: storage}, nil
}

// Consume 消费消息，实现 kq.Pusher 接口
func (h *TraceHandler) Consume(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var spans []TraceSpan
	if err := h.parseMessage(value, &spans); err != nil {
		logx.Errorf("解析trace消息失败: %v", err)
		return nil // 跳过无效消息，不重试
	}

	if len(spans) == 0 {
		return nil
	}

	return h.storage.StoreBatch(ctx, spans)
}

// parseMessage 解析消息
func (h *TraceHandler) parseMessage(value string, spans *[]TraceSpan) error {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return fmt.Errorf("JSON解析失败: %w", err)
	}

	// 尝试 OTLP 格式: {"batch": [...]}
	if batch, ok := data["batch"].([]interface{}); ok {
		batchBytes, _ := json.Marshal(batch)
		if err := json.Unmarshal(batchBytes, spans); err != nil {
			return fmt.Errorf("解析batch失败: %w", err)
		}
		return nil
	}

	// 尝试直接 spans 数组
	if spanData, ok := data["spans"].([]interface{}); ok {
		spanBytes, _ := json.Marshal(spanData)
		if err := json.Unmarshal(spanBytes, spans); err != nil {
			return fmt.Errorf("解析spans失败: %w", err)
		}
		return nil
	}

	return fmt.Errorf("未知消息格式")
}

// Stop 停止 handler
func (h *TraceHandler) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.storage.Close(ctx)
	logx.Info("trace handler 已停止")
}
