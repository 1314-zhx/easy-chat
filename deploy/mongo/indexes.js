// mongo "mongodb://root:easy-chat@127.0.0.1:27017/easy-chat?authSource=admin" deploy/mongo/indexes.js
// 与 apps/im/immodels/indexes.go 保持一致。

db = db.getSiblingDB("easy-chat");

db.chat_log.createIndex({ conversationId: 1, seq: 1 }, { name: "idx_conv_seq" });
db.chat_log.createIndex({ conversationId: 1, sendTime: -1 }, { name: "idx_conv_sendTime" });

db.conversation.createIndex({ conversationId: 1 }, { name: "idx_conversationId" });
db.conversations.createIndex({ userId: 1 }, { unique: true, name: "uk_userId" });

db.user_conversation_read.createIndex(
  { userId: 1, conversationId: 1 },
  { unique: true, name: "uk_user_conv" }
);

db.conversation_topics.createIndex({ conversationId: 1, status: 1 }, { name: "idx_conv_status" });
db.conversation_topics.createIndex({ topicId: 1 }, { unique: true, name: "uk_topicId" });

db.message_vectors.createIndex({ chatLogId: 1 }, { unique: true, name: "uk_chatLogId" });
db.message_vectors.createIndex({ conversationId: 1, sendTime: -1 }, { name: "idx_conv_sendTime" });

db.user_memory.createIndex(
  { userId: 1, conversationId: 1 },
  { unique: true, name: "uk_user_conv" }
);
