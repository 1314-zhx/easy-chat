/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package user

import (
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/apps/im/ws/websocket"
)

// 获取所有在线用户
func OnLine(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		uids := srv.GetUsers()  // GetUsers 如果传参为 nil，获取当前服务器上的全部用户
		u := srv.GetUsers(conn) // 请求的用户
		err := srv.Send(websocket.NewMessage(u[0], uids), conn)
		srv.Info("err ", err)
	}
}
