-- Existing easy_chat DB: add hot-path indexes (MySQL 5.7).
-- uk_group_user: GroupUsers / ListByGroupId on every group message.
-- idx_user_id: ListByUserId (groups I joined).
-- uk_user_friend: friend list and FindByUidAndFid.

ALTER TABLE `group_members`
  ADD UNIQUE KEY `uk_group_user` (`group_id`,`user_id`),
  ADD KEY `idx_user_id` (`user_id`);

ALTER TABLE `friends`
  ADD UNIQUE KEY `uk_user_friend` (`user_id`,`friend_uid`);
