// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package friend

import (
	"context"
	"easy-chat/apps/social/rpc/social"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/ctxdata"
	"fmt"

	"easy-chat/apps/social/api/internal/svc"
	"easy-chat/apps/social/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type FriendsOnlineLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 好友在线情况
func NewFriendsOnlineLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FriendsOnlineLogic {
	return &FriendsOnlineLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *FriendsOnlineLogic) FriendsOnline(req *types.FriendsOnlineReq) (resp *types.FriendsOnlineResp, err error) {
	// todo: add your logic here and delete this line
	uid := ctxdata.GetUId(l.ctx)

	friendList, err := l.svcCtx.Social.FriendList(l.ctx, &social.FriendListReq{
		UserId: uid,
	})
	fmt.Println("................", friendList)
	if err != nil || len(friendList.List) == 0 {
		return &types.FriendsOnlineResp{}, err
	}

	// 在缓存中查询在线用户
	Fids := make([]string, 0, len(friendList.List))
	for _, friend := range friendList.List {
		Fids = append(Fids, friend.FriendUid)
	}
	// 获取在线用户
	onlines, err := l.svcCtx.Redis.Hgetall(constants.REDIS_ONLINE_USER)
	if err != nil {
		return nil, err
	}
	resOnLineList := make(map[string]bool, len(Fids))
	for _, fid := range Fids {
		if _, ok := onlines[fid]; ok {
			// 如果用户存在就设置为 true
			resOnLineList[fid] = true
		} else {
			//  如果用户不存在就设置为 false
			resOnLineList[fid] = false
		}

	}
	return &types.FriendsOnlineResp{
		OnlineList: resOnLineList,
	}, nil
}
