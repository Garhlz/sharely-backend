package com.szu.afternoon3.platform.common;

/** 仅供迁移前禁用用例编译，不属于当前 Java 业务契约。 */
public interface RedisKey {
    // Set结构: 存储点赞用户ID (用于快速去重/判断是否点赞)
    // Key: inter:post:like:{postId}, Member: userId
    String POST_LIKE_SET = "inter:post:like:";

    // Set结构: 存储收藏用户ID
    // Key: inter:post:collect:{postId}, Member: userId
    String POST_COLLECT_SET = "inter:post:collect:";

    // Hash结构: 存储评分 (用于快速获取用户是否评过分，以及评了多少分)
    // Key: inter:post:rate:{postId}, Field: userId, Value: score
    String POST_RATE_HASH = "inter:post:rate:";

    // Key: inter:comment:like:{commentId}, Member: userId
    String COMMENT_LIKE_SET = "inter:comment:like:";
}
