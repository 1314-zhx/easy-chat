/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package constants

import "fmt"

type MType int

const (
	TextMType MType = iota
)

type ChatType int

const (
	GroupChatType ChatType = iota + 1
	SingleChatType
)

// String 实现 fmt.Stringer 接口，用于埋点维度标签
func (c ChatType) String() string {
	switch c {
	case SingleChatType:
		return "single_chat"
	case GroupChatType:
		return "group_chat"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

type ContentType int

const (
	ContentChatType ContentType = iota
	ContentMarkRead
	ContentConversationRefresh
)
