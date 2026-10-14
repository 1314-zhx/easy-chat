package redis

// 本文件展示 ConversationCacheV2 的使用方式
// 实际测试请参考 conversation_cache_v2_test.go

// -------------------- 使用示例 --------------------

/*
// 1. 获取单个会话（包含内嵌消息）
func GetConversationExample(cache *ConversationCacheV2, userId, convId string) {
	conv, err := cache.HGet(userId, convId)
	if err != nil {
		return
	}
	if conv == nil {
		// 会话不存在
		return
	}

	// 访问会话信息
	println(conv.TargetId)
	println(conv.TargetName)
	println(conv.Unread)

	// 访问最近消息（已在缓存中）
	for _, msg := range conv.Messages {
		println(msg.Content)
		println(msg.Time)
	}
}

// 2. 获取用户所有会话
func GetAllConversationsExample(cache *ConversationCacheV2, userId string) {
	all, err := cache.HGetAll(userId)
	if err != nil {
		return
	}

	for convId, conv := range all {
		println(convId, conv.TargetName)
		println("消息数:", len(conv.Messages))
	}
}

// 3. 设置会话缓存
func SetConversationExample(cache *ConversationCacheV2) {
	conv := &ConversationData{
		ConversationId: "conv_001",
		ChatType:       1, // 单聊
		TargetId:       "user_456",
		TargetName:     "张三",
		TargetIcon:     "http://xxx/avatar.png",
		IsShow:         true,
		Seq:            100,
		Unread:         5,
		Messages: []*MsgData{
			{Id: "msg_001", Content: "你好", Time: 1699999999},
			{Id: "msg_002", Content: "在吗", Time: 1700000000},
		},
	}

	err := cache.HSet("user_123", conv)
	if err != nil {
		println("设置失败:", err.Error())
	}
}

// 4. 追加消息（原子操作，自动截取保留最近 50 条）
func AppendMessageExample(cache *ConversationCacheV2) {
	msg := &MsgData{
		Id:       "msg_new",
		SendId:   "user_123",
		SendName: "我",
		Content:  "新消息内容",
		Time:     1700000100,
		Type:     1,
	}

	err := cache.AppendMessage("user_123", "conv_001", msg)
	if err != nil {
		println("追加失败:", err.Error())
	}
}

// 5. 追加消息并更新会话元信息（原子操作）
func AppendMessageWithMetaExample(cache *ConversationCacheV2) {
	msg := &MsgData{
		Id:       "msg_new",
		SendId:   "user_456",
		SendName: "张三",
		Content:  "回复内容",
		Time:     1700000200,
		Type:     1,
	}

	meta := &ConversationMeta{
		ChatType:   1,
		TargetId:   "user_456",
		TargetName: "张三",
		Seq:        101,
		Total:      1001,
		Unread:     6,
	}

	err := cache.AppendMessageWithMeta("user_123", "conv_001", msg, meta)
	if err != nil {
		println("追加失败:", err.Error())
	}
}

// 6. 单独更新会话元信息
func UpdateMetaExample(cache *ConversationCacheV2) {
	meta := &ConversationMeta{
		ChatType:   1,
		TargetId:   "user_456",
		TargetName: "张三",
		Unread:     0, // 已读
	}

	err := cache.UpdateMeta("user_123", "conv_001", meta)
	if err != nil {
		println("更新失败:", err.Error())
	}
}

// 7. 更新未读数
func UpdateUnreadExample(cache *ConversationCacheV2) {
	err := cache.UpdateUnread("user_123", "conv_001", 0)
	if err != nil {
		println("更新失败:", err.Error())
	}
}

// 8. 删除会话缓存
func DeleteConversationExample(cache *ConversationCacheV2) {
	// 删除单个会话
	err := cache.HDel("user_123", "conv_001")
	if err != nil {
		println("删除失败:", err.Error())
	}

	// 删除所有会话
	err = cache.HDelAll("user_123")
	if err != nil {
		println("删除失败:", err.Error())
	}
}

// 9. 延迟双删策略（推荐在写操作时使用）
func DeleteWithDelayExample(cache *ConversationCacheV2) {
	cache.DelAsync("user_123", 500*time.Millisecond)
}

// -------------------- 业务流程整合示例 --------------------

/*
// 场景：用户发送消息后，更新会话缓存
func OnMessageSentExample(cache *ConversationCacheV2, userId string, convId string, msg *MsgData) {
	// 方法一：只追加消息（消息已写入 MongoDB）
	err := cache.AppendMessage(userId, convId, msg)
	if err != nil {
		log.Println("更新缓存失败", err)
	}

	// 方法二：追加消息并更新元信息（一次性原子操作）
	// 注意：meta 中的字段会覆盖现有值
	meta := &ConversationMeta{
		Seq:    msg.Seq,
		Total:  msg.Total,
		Unread: 0,
	}
	err = cache.AppendMessageWithMeta(userId, convId, msg, meta)
}

// 场景：拉取会话列表时，填充缓存
func OnGetConversationsExample(cache *ConversationCacheV2, userId string, dbConvs []*ConversationData) {
	for _, conv := range dbConvs {
		err := cache.HSetWithTTL(userId, conv)
		if err != nil {
			log.Println("填充缓存失败", err)
		}
	}
}

// 场景：用户进入聊天页面，从缓存获取消息
func OnEnterChatExample(cache *ConversationCacheV2, userId, convId string) []*MsgData {
	// 1. 先查缓存（包含最近 50 条消息）
	conv, err := cache.HGet(userId, convId)
	if err != nil {
		return nil
	}

	// 2. 缓存命中，直接返回
	if conv != nil && len(conv.Messages) > 0 {
		return conv.Messages
	}

	// 3. 缓存未命中，返回空（前端需要从 MongoDB 加载更多历史消息）
	return nil
}

// 场景：用户已读消息
func OnMessageReadExample(cache *ConversationCacheV2, userId, convId string) {
	err := cache.UpdateUnread(userId, convId, 0)
	if err != nil {
		log.Println("更新未读数失败", err)
	}
}
*/
