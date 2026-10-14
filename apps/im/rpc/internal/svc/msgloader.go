package svc

import (
	"easy-chat/apps/im/convload"
	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/redis"
)

func ChatLogToPoolItem(c *immodels.ChatLog) *redis.MsgPoolItem {
	return convload.ChatLogToPoolItem(c)
}

func BuildMsgLoader(model immodels.ChatLogModel) redis.MsgLoader {
	return convload.BuildMsgLoader(model)
}
