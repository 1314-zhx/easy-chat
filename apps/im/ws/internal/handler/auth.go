/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package handler

import (
	"context"
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/pkg/ctxdata"
	"net/http"

	"github.com/golang-jwt/jwt/v4"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/token"
)

type JwtAuth struct {
	svc    *svc.ServiceContext
	parser *token.TokenParser
	logx.Logger
}

func NewJwtAuth(svc *svc.ServiceContext) *JwtAuth {
	return &JwtAuth{
		svc:    svc,
		parser: token.NewTokenParser(),
		Logger: logx.WithContext(context.Background()),
	}
}

func (j *JwtAuth) Auth(w http.ResponseWriter, r *http.Request) bool {
	// 1. 解析 JWT Token（从 HTTP Header 或 URL 参数）
	tok, err := j.parser.ParseToken(r, j.svc.RuntimeConfig().JwtAuth.AccessSecret, "")
	if err != nil {
		j.Errorf("parse token err %v ", err)
		return false
	}
	// 2. 验证 Token 有效性
	if !tok.Valid {
		return false
	}
	// 3. 提取 Claims
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return false
	}
	// 将解析后的 claims 注入请求上下文中，以 key-value 的方式存储 claims
	*r = *r.WithContext(context.WithValue(r.Context(), ctxdata.Identify, claims[ctxdata.Identify]))

	return true
}

// 传入上下文，获取用户的 id
func (j *JwtAuth) UserId(r *http.Request) string {
	return ctxdata.GetUId(r.Context())
}
