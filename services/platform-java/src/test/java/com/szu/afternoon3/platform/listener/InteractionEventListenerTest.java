package com.szu.afternoon3.platform.listener;

import com.szu.afternoon3.platform.entity.User;
import com.szu.afternoon3.platform.entity.mongo.NotificationDoc;
import com.szu.afternoon3.platform.enums.NotificationType;
import com.szu.afternoon3.platform.event.InteractionEvent;
import com.szu.afternoon3.platform.grpc.NotificationRpcClient;
import com.szu.afternoon3.platform.mapper.UserMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class InteractionEventListenerTest {
    private InteractionEventListener listener;
    private UserMapper users;
    private NotificationRpcClient notifications;

    @BeforeEach
    void setUp() {
        listener = new InteractionEventListener();
        users = mock(UserMapper.class);
        notifications = mock(NotificationRpcClient.class);
        ReflectionTestUtils.setField(listener, "userMapper", users);
        ReflectionTestUtils.setField(listener, "notificationRpcClient", notifications);
    }

    @Test
    void followStillCreatesNotificationThroughGoAdapter() {
        User sender = new User();
        sender.setId(7L);
        sender.setNickname("sender");
        sender.setAvatar("avatar");
        when(users.selectById(7L)).thenReturn(sender);

        listener.handleInteraction(new InteractionEvent(7L, "9", "FOLLOW", "ADD", null));

        ArgumentCaptor<NotificationDoc> saved = ArgumentCaptor.forClass(NotificationDoc.class);
        verify(notifications).save(saved.capture());
        assertThat(saved.getValue().getReceiverId()).isEqualTo(9L);
        assertThat(saved.getValue().getSenderId()).isEqualTo(7L);
        assertThat(saved.getValue().getType()).isEqualTo(NotificationType.FOLLOW);
        assertThat(saved.getValue().getTargetId()).isEqualTo("7");
    }

    @Test
    void migratedInteractionsAreNotProcessedAgainByJava() {
        for (String type : new String[]{"LIKE", "COLLECT", "RATE", "COMMENT_LIKE"}) {
            listener.handleInteraction(new InteractionEvent(7L, "post", type, "ADD", null));
        }
        listener.handleInteraction(new InteractionEvent(7L, "9", "FOLLOW", "REMOVE", null));
        verifyNoInteractions(users, notifications);
    }

    @Test
    void invalidFollowTargetDoesNotCreateNotification() {
        listener.handleInteraction(new InteractionEvent(7L, "bad", "FOLLOW", "ADD", null));
        verifyNoInteractions(users, notifications);
    }

    @Test
    void missingSenderDoesNotCreateNotification() {
        listener.handleInteraction(new InteractionEvent(7L, "9", "FOLLOW", "ADD", null));
        verifyNoInteractions(notifications);
    }
}
