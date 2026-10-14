/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import (
	"fmt"
	"net/http"
	"time"
)

type Authentication interface {
	Auth(w http.ResponseWriter, r *http.Request) bool
	UserId(r *http.Request) string
}

// 默认的实现方式
type authentication struct{}

func (*authentication) Auth(w http.ResponseWriter, r *http.Request) bool {
	return true
}

// 获取Id
func (*authentication) UserId(r *http.Request) string {
	query := r.URL.Query()
	// 如果 URL 中有Id
	if query != nil && query["userId"] != nil {
		return fmt.Sprintf("%v", query["userId"])
	}
	// 或者用时间戳作为请求的 Id
	return fmt.Sprintf("%v", time.Now().UnixMilli())
}
