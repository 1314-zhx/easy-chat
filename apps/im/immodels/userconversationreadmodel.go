package immodels

import "github.com/zeromicro/go-zero/core/stores/mon"

var _ UserConversationReadModel = (*customUserConversationReadModel)(nil)

type (
	// UserConversationReadModel is an interface to be customized, add more methods here,
	// and implement the added methods in customUserConversationReadModel.
	UserConversationReadModel interface {
		userConversationReadModel
	}

	customUserConversationReadModel struct {
		*defaultUserConversationReadModel
	}
)

// NewUserConversationReadModel returns a model for the mongo.
func NewUserConversationReadModel(url, db, collection string) UserConversationReadModel {
	conn := mon.MustNewModel(url, db, collection)
	return &customUserConversationReadModel{
		defaultUserConversationReadModel: newDefaultUserConversationReadModel(conn),
	}
}

func MustUserConversationReadModel(url, db string) UserConversationReadModel {
	return NewUserConversationReadModel(url, db, "user_conversation_read")
}
