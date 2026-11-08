package com.szu.afternoon3.platform.listener;

import com.szu.afternoon3.platform.config.RabbitConfig;
import com.szu.afternoon3.platform.entity.User;
import com.szu.afternoon3.platform.entity.mongo.NotificationDoc;
import com.szu.afternoon3.platform.enums.NotificationType;
import com.szu.afternoon3.platform.event.InteractionEvent;
import com.szu.afternoon3.platform.grpc.NotificationRpcClient;
import com.szu.afternoon3.platform.mapper.UserMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 互动持久化已迁入 Go；此处仅保留 Java 关注业务的通知适配。 */
@Component
@Slf4j
public class InteractionEventListener {
    @Autowired private UserMapper userMapper;
    @Autowired private NotificationRpcClient notificationRpcClient;

    @RabbitListener(queues = RabbitConfig.QUEUE_INTERACTION)
    public void handleInteraction(InteractionEvent event) {
        if ("FOLLOW".equals(event.getType()) && "ADD".equals(event.getAction())) {
            sendFollowNotification(event);
        } else {
            // 迁移前必须排空旧互动事件，禁止在此重复写入已由 Go 提交的互动。
            log.warn("忽略旧互动事件，请检查迁移切换流程: type={}, targetId={}", event.getType(), event.getTargetId());
        }
    }

    private void sendFollowNotification(InteractionEvent event) {
        Long senderId = event.getUserId();
        String targetUserIdStr = event.getTargetId();

        // 1. 解析被关注者的ID
        long receiverId;
        try {
            receiverId = Long.parseLong(targetUserIdStr);
        } catch (NumberFormatException e) {
            log.warn("关注通知目标ID格式错误: {}", targetUserIdStr);
            return;
        }

        // 2. 查发起者信息 (粉丝的信息)
        User sender = userMapper.selectById(senderId);
        if (sender == null) return;

        // 3. 构建通知
        NotificationDoc doc = new NotificationDoc();
        doc.setReceiverId(receiverId); // 通知的接收者 = 被关注的人

        doc.setSenderId(sender.getId());
        doc.setSenderNickname(sender.getNickname());
        doc.setSenderAvatar(sender.getAvatar());

        doc.setType(NotificationType.FOLLOW);
        doc.setTargetId(String.valueOf(senderId)); // 关联的目标ID就是粉丝的ID，点击可能跳到粉丝主页
        doc.setTargetPreview("关注了你"); // 简单文案

        // 4. 保存
        notificationRpcClient.save(doc);
    }
}
