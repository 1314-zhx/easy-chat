package msgTransfer

import (
	"context"
	"easy-chat/apps/ai/rpc/ai"
	"easy-chat/apps/ai/rpc/aiagent"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/ws/ws"
	"easy-chat/apps/task/mq/internal/svc"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/apps/task/mq/pkg/dlq"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/metrics"
	"easy-chat/pkg/qwen"
	msgredis "easy-chat/pkg/redis"
	"easy-chat/pkg/tools"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/zrpc"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	MsgChatTransferTopic = "msgChatTransfer"   // 原始 topic
	MsgChatTransferDLT   = "msgChatTransferDLT" // 死信 topic
)

type MsgChatTransfer struct {
	*baseMsgTransfer
	aiClient       aiagent.AiAgent
	toolReg        *tools.Registry
	dlq            *dlq.KafkaDLQProducer
	batchProcessor *BatchProcessor
	commits        *offsetCommitter
}

func NewMsgChatTransfer(svc *svc.ServiceContext) *MsgChatTransfer {
	var aiClient aiagent.AiAgent
	if svc.AiRpc.Etcd.Key != "" {
		aiClient = aiagent.NewAiAgent(zrpc.MustNewClient(svc.AiRpc))
	}

	// 初始化死信队列生产者
	var dlqProducer *dlq.KafkaDLQProducer
	if svc.KafkaBrokers != nil && len(svc.KafkaBrokers) > 0 {
		var err error
		dlqProducer, err = dlq.NewKafkaDLQProducer(svc.KafkaBrokers, MsgChatTransferDLT)
		if err != nil {
			fmt.Printf("Warning: failed to create DLQ producer: %v\n", err)
		}
	}

	m := &MsgChatTransfer{
		baseMsgTransfer: NewBaseMsgTransfer(svc),
		aiClient:        aiClient,
		toolReg:         tools.NewRegistry(tools.Options{}),
		dlq:             dlqProducer,
	}
	m.batchProcessor = NewBatchProcessor(
		svc.ChatLogCustom,
		svc.ConversationModel,
		dlqProducer,
		MsgChatTransferTopic,
		defaultBatchSize,
		defaultBatchTimeout,
		m.onMongoInsertComplete,
	)
	return m
}

// Consume 仅解析并放入批处理缓冲，不等待落库，也不提交位点。
func (m *MsgChatTransfer) Consume(key, value string) error {
	return m.accept(kafka.Message{Key: []byte(key), Value: []byte(value)})
}

// accept 持续消费的入口。带消费组位点的消息会在对应批次落库成功后才提交。
func (m *MsgChatTransfer) accept(msg kafka.Message) error {
	if m.commits != nil {
		m.commits.track(msg)
	}

	var (
		data  mq.MsgChatTransfer
		msgId = primitive.NewObjectID()
	)
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		metrics.KafkaProduceTotal.WithLabelValues("msg_chat_transfer", "failed").Inc()
		if m.commits != nil {
			m.commits.markDone(msg)
		}
		return err
	}

	if qwen.IsAIMessage(data.RecvId) && m.aiClient != nil {
		copied := data
		go m.finishAI(msg, &copied)
		return nil
	}

	dataCopy := data
	if err := m.batchProcessor.Submit(&BatchItem{
		ChatLog:   m.prepareChatLog(msgId, &data),
		Data:      &dataCopy,
		MsgId:     msgId,
		Key:       string(msg.Key),
		Value:     string(msg.Value),
		Msg:       msg,
		HasCommit: m.commits != nil,
	}); err != nil {
		metrics.KafkaProduceTotal.WithLabelValues("msg_chat_transfer", "failed").Inc()
		return err
	}
	metrics.KafkaProduceTotal.WithLabelValues("msg_chat_transfer", "success").Inc()
	return nil
}

func (m *MsgChatTransfer) finishAI(msg kafka.Message, data *mq.MsgChatTransfer) {
	err := m.HandleAiMessage(context.Background(), data)
	if err != nil {
		m.Errorf("ai message handled with error, offset still advanced with batch watermark: %v", err)
	}
	if m.commits != nil {
		m.commits.markDone(msg)
	}
}

func (m *MsgChatTransfer) onMongoInsertComplete(item *BatchItem, dbErr error) {
	if item == nil || item.Data == nil {
		return
	}
	data := item.Data
	if dbErr == nil && item.ChatLog != nil {
		if afterErr := m.afterChatLogInserted(context.Background(), item.ChatLog, data); afterErr != nil {
			m.Errorf("afterChatLogInserted error: %v", afterErr)
		}
	}

	if data.ClientMsgId != "" {
		if notifyErr := m.sendDbAck(data, dbErr); notifyErr != nil {
			m.Errorf("Failed to send MongoDB ack notification, clientMsgId: %s, err: %v",
				data.ClientMsgId, notifyErr)
		} else {
			m.Infof("MongoDB ack sent, clientMsgId: %s, success: %v", data.ClientMsgId, dbErr == nil)
		}
	}

	if dbErr != nil || item.ChatLog == nil {
		return
	}

	pushStart := time.Now()
	pushErr := m.Transfer(context.Background(), &ws.Push{
		ConversationId: data.ConversationId,
		ChatType:       data.ChatType,
		SendId:         data.SendId,
		RecvId:         data.RecvId,
		RecvIds:        data.RecvIds,
		SendTime:       data.SendTime,
		MType:          data.MType,
		MsgId:          item.MsgId.Hex(),
		Content:        data.Content,
		Seq:            chatLogSeq(data, item.ChatLog),
	})
	if pushErr == nil {
		metrics.MessagePushLatency.WithLabelValues(data.ChatType.String(), "push").Observe(
			float64(time.Since(pushStart).Milliseconds()),
		)
		metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "success").Inc()
	} else {
		metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "failed").Inc()
		metrics.ErrorTotal.WithLabelValues("websocket").Inc()
		m.Errorf("push after mongo insert failed: %v", pushErr)
	}
}

func (m *MsgChatTransfer) HandleAiMessage(ctx context.Context, data *mq.MsgChatTransfer) error {
	aiStart := time.Now()
	m.Infof("处理AI消息: user=%s, msg=%s, conversation=%s", data.SendId, data.Content, data.ConversationId)

	userMsgId := primitive.NewObjectID()
	userChatLog := immodels.ChatLog{
		ID: userMsgId, ConversationId: data.ConversationId,
		SendId: data.SendId, RecvId: data.RecvId, ChatType: data.ChatType,
		MsgFrom: 0, MsgType: data.MType, MsgContent: data.Content, SendTime: time.Now().UnixNano(),
		ClientMsgId: data.ClientMsgId,
	}

	// 埋点: 用户消息入库
	insertStart := time.Now()
	if err := m.svcCtx.ChatLogModel.Insert(ctx, &userChatLog); err != nil {
		metrics.MongoOperationLatency.WithLabelValues("insert", "chatlog").Observe(
			float64(time.Since(insertStart).Milliseconds()),
		)
		metrics.MongoOperationTotal.WithLabelValues("insert", "chatlog", "failed").Inc()
	} else {
		metrics.MongoOperationLatency.WithLabelValues("insert", "chatlog").Observe(
			float64(time.Since(insertStart).Milliseconds()),
		)
		metrics.MongoOperationTotal.WithLabelValues("insert", "chatlog", "success").Inc()
	}

	m.indexChatLogAsync(&userChatLog)

	userSendTime := userChatLog.SendTime
	resp, err := m.aiClient.Chat(ctx, &ai.ChatRequest{
		UserId:          data.SendId,
		Message:         data.Content,
		ConversationId: data.ConversationId,
		ExcludeSendTime: userSendTime,
	})

	finalReply := "抱歉，AI 暂时无法响应"
	if err == nil && resp.Success {
		if resp.NeedAction && resp.ToolCall != nil {
			m.Infof("执行工具: %s, 参数: %v", resp.ToolCall.Name, resp.ToolCall.Arguments)
			result, execErr := m.toolReg.Execute(ctx, resp.ToolCall.Name, resp.ToolCall.Arguments, data.Content)
			if execErr != nil {
				result = execErr.Error()
			}

			finalResp, _ := m.aiClient.Chat(ctx, &ai.ChatRequest{
				UserId:          data.SendId,
				Message:         data.Content,
				ConversationId:  data.ConversationId,
				ExcludeSendTime: userSendTime,
				ToolResult:      fmt.Sprintf("[%s] 结果: %s", resp.ToolCall.Name, result),
			})
			if finalResp != nil && finalResp.Success && finalResp.Reply != "" {
				finalReply = finalResp.Reply
			} else {
				finalReply = result
			}
		} else {
			finalReply = resp.Reply
		}
	}

	// 埋点: AI响应延迟
	metrics.KafkaProduceLatency.WithLabelValues("ai_response", "success").Observe(
		float64(time.Since(aiStart).Milliseconds()),
	)

	aiMsgId := primitive.NewObjectID()
	aiChatLog := immodels.ChatLog{
		ID: aiMsgId, ConversationId: data.ConversationId,
		SendId: "ai_assistant", RecvId: data.SendId, ChatType: constants.SingleChatType,
		MsgFrom: 1, MsgType: constants.TextMType, MsgContent: finalReply, SendTime: time.Now().UnixNano(),
	}

	// 埋点: AI消息入库
	aiInsertStart := time.Now()
	if err := m.svcCtx.ChatLogModel.Insert(ctx, &aiChatLog); err != nil {
		metrics.MongoOperationLatency.WithLabelValues("insert", "chatlog").Observe(
			float64(time.Since(aiInsertStart).Milliseconds()),
		)
		metrics.MongoOperationTotal.WithLabelValues("insert", "chatlog", "failed").Inc()
	} else {
		metrics.MongoOperationLatency.WithLabelValues("insert", "chatlog").Observe(
			float64(time.Since(aiInsertStart).Milliseconds()),
		)
		metrics.MongoOperationTotal.WithLabelValues("insert", "chatlog", "success").Inc()
		m.indexChatLogAsync(&aiChatLog)
		m.publishTopicRound(data, userMsgId.Hex(), aiMsgId.Hex(), data.Content, finalReply)
		_ = m.svcCtx.ConversationModel.UpdateMsg(ctx, &aiChatLog)
		if m.svcCtx.ConvSession != nil {
			_ = m.svcCtx.ConvSession.InvalidateOnNewMessage(data.ConversationId, &msgredis.MsgPoolItem{
				Seq:     aiChatLog.Seq,
				MsgId:   aiChatLog.ID.Hex(),
				SendId:  "ai_assistant",
				Content: finalReply,
				Type:    int32(constants.TextMType),
				Time:    aiChatLog.SendTime,
			})
		}
	}

	// 埋点: AI消息推送
	aiPushStart := time.Now()
	pushErr := m.Transfer(ctx, &ws.Push{
		ConversationId: data.ConversationId, ChatType: constants.SingleChatType,
		SendId: "ai_assistant", RecvId: data.SendId, SendTime: time.Now().UnixNano(),
		MType: constants.TextMType, MsgId: aiMsgId.Hex(), Content: finalReply,
		Seq: aiChatLog.Seq,
	})

	if pushErr == nil {
		metrics.MessagePushLatency.WithLabelValues(constants.SingleChatType.String(), "push").Observe(
			float64(time.Since(aiPushStart).Milliseconds()),
		)
		metrics.MessagePushTotal.WithLabelValues(constants.SingleChatType.String(), "success").Inc()
	} else {
		metrics.MessagePushTotal.WithLabelValues(constants.SingleChatType.String(), "failed").Inc()
	}

	return pushErr
}

func (m *MsgChatTransfer) publishTopicRound(data *mq.MsgChatTransfer, userMsgId, aiMsgId, userContent, aiContent string) {
	if m.svcCtx.TopicRoundClient == nil {
		return
	}
	_ = m.svcCtx.TopicRoundClient.Push(&mq.TopicRoundComplete{
		ConversationId: data.ConversationId,
		UserId:         data.SendId,
		RoundId:        uuid.NewString(),
		UserMsgId:      userMsgId,
		AiMsgId:        aiMsgId,
		UserContent:    userContent,
		AiContent:      aiContent,
	})
}

func (m *MsgChatTransfer) indexChatLogAsync(log *immodels.ChatLog) {
	if m.svcCtx.MessageIndexer == nil || log == nil {
		return
	}
	idx := m.svcCtx.MessageIndexer
	copyLog := *log
	go func() {
		_ = idx.IndexChatLog(context.Background(), &copyLog)
	}()
}

// sendDbAck 发送 MongoDB 落库结果通知给 WebSocket Server
// 用于三次 ACK 机制：通知 WebSocket Server MongoDB 落库是否成功
func (m *MsgChatTransfer) sendDbAck(data *mq.MsgChatTransfer, dbErr error) error {
	// 通过 WebSocket Client 发送通知
	// SendDbAck 会构建正确的消息格式并发送
	return m.svcCtx.WsClient.SendDbAck(data.ClientMsgId, dbErr)
}

// prepareChatLog 组装 ChatLog。seq 在插入事务中按会话 $inc 后写入，避免先占号后插失败造成空洞。
func (m *MsgChatTransfer) prepareChatLog(msgId primitive.ObjectID, data *mq.MsgChatTransfer) *immodels.ChatLog {
	return &immodels.ChatLog{
		ID:             msgId,
		ConversationId: data.ConversationId,
		SendId:         data.SendId,
		RecvId:         data.RecvId,
		ChatType:       data.ChatType,
		MsgFrom:        0,
		MsgType:        data.MType,
		MsgContent:     data.Content,
		SendTime:       data.SendTime,
		ClientMsgId:    data.ClientMsgId,
	}
}

// afterChatLogInserted 落库成功后：更新会话 lastMsg、三层缓存、RAG 索引
func (m *MsgChatTransfer) afterChatLogInserted(ctx context.Context, chatLog *immodels.ChatLog, data *mq.MsgChatTransfer) error {
	if err := m.svcCtx.ConversationModel.UpdateMsg(ctx, chatLog); err != nil {
		return err
	}

	m.msgIndexAsync(chatLog)

	if m.svcCtx.ConvSession != nil {
		if err := m.svcCtx.ConvSession.InvalidateOnNewMessage(data.ConversationId, &msgredis.MsgPoolItem{
			Seq:     chatLog.Seq,
			MsgId:   chatLog.ID.Hex(),
			SendId:  data.SendId,
			Content: data.Content,
			Type:    int32(data.MType),
			Time:    data.SendTime,
		}); err != nil {
			// 库已成功：缓存失败只降级旁路（内部已 DEL），不失败本次消费
			m.Errorf("conv cache dual-write failed, degraded to cache-aside: %v", err)
		}
	}
	return nil
}

func chatLogSeq(data *mq.MsgChatTransfer, chatLog *immodels.ChatLog) int64 {
	if chatLog != nil && chatLog.Seq > 0 {
		return chatLog.Seq
	}
	if data == nil {
		return 0
	}
	return 0
}

func (m *MsgChatTransfer) sendToDLQ(key, value, errMsg string) {
	if m.dlq == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := m.dlq.SendToDLQ(ctx, MsgChatTransferTopic, key, value, errMsg); e != nil {
			m.Errorf("send DLQ failed: %v", e)
		}
	}()
}

// isConversationActive 判断用户是否打开着某个会话（推送降噪用）
func (m *MsgChatTransfer) isConversationActive(userId, conversationId string) (bool, error) {
	if m.svcCtx.ActiveConversationCache == nil {
		return false, nil
	}
	return m.svcCtx.ActiveConversationCache.IsMember(userId, conversationId)
}

// msgIndexAsync RAG 索引（异步）
func (m *MsgChatTransfer) msgIndexAsync(log *immodels.ChatLog) {
	if m.svcCtx.MessageIndexer == nil || log == nil {
		return
	}
	idx := m.svcCtx.MessageIndexer
	copyLog := *log
	go func() {
		_ = idx.IndexChatLog(context.Background(), &copyLog)
	}()
}
