package com.szu.afternoon3.platform.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class InteractionEvent {
    // 当前仅用于 Java FOLLOW 通知；保留旧字段形状以兼容历史消息反序列化。
    /**
     * 触发交互的用户ID
     */
    private Long userId;

    /**
     * 目标ID (帖子ID 或 评论ID 或目标用户ID)
     */
    private String targetId;

    /**
     * 交互类型: LIKE(点赞), COLLECT(收藏), RATE(评分), FOLLOW(关注)
     */
    private String type;

    /**
     * 动作: ADD(新增), REMOVE(取消/移除)
     * 对于评分，ADD 代表新增或更新
     */
    private String action;

    /**
     * 数值 (仅用于评分 RATE，其他类型为 null)
     */
    private Double value;
}
