package com.szu.afternoon3.platform.controller;
import org.junit.jupiter.api.Disabled;

import cn.hutool.core.lang.UUID;
import cn.hutool.json.JSONUtil;
import com.szu.afternoon3.platform.common.RedisKey;
import com.szu.afternoon3.platform.dto.PostRateDTO;
import com.szu.afternoon3.platform.entity.User;
import com.szu.afternoon3.platform.entity.mongo.*;
import com.szu.afternoon3.platform.mapper.UserMapper;
import com.szu.afternoon3.platform.repository.*;
import com.szu.afternoon3.platform.util.JwtUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@org.junit.jupiter.api.Tag("integration")
@SpringBootTest
@AutoConfigureMockMvc
@Transactional // 保持事务回滚，防止垃圾数据堆积
@Disabled("互动已迁移到 Go；场景对应关系见 docs/testing.md，禁止直接启用旧路由与共享库清理夹具")
public class InteractionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private UserMapper userMapper;
    @Autowired
    private JwtUtil jwtUtil;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired private PostRepository postRepository;
    @Autowired private PostLikeRepository postLikeRepository;
    @Autowired private PostCollectRepository postCollectRepository;
    @Autowired private PostRatingRepository postRatingRepository;
    @Autowired private CommentRepository commentRepository;
    @Autowired private CommentLikeRepository commentLikeRepository;

    private String token;
    private Long userId;
    private String postId;

    @BeforeEach
    public void setup() {
        // 1. 清理 NoSQL 数据 (Mongo/Redis 不受 Transactional 控制，必须手动清)
        clearNoSqlData();

        // 2. 【核心修改】生成随机邮箱，防止与逻辑删除的历史数据冲突
        String randomEmail = "test_" + UUID.randomUUID().toString().substring(0, 8) + "@szu.edu.cn";

        // 3. 创建测试用户
        User user = new User();
        user.setNickname("交互测试员");
        user.setEmail(randomEmail); // 使用随机邮箱
        user.setStatus(1);
        userMapper.insert(user);
        this.userId = user.getId();

        // 4. 生成 Token
        this.token = "Bearer " + jwtUtil.createAccessToken(userId, "USER", user.getNickname());

        // 5. 创建测试帖子
        PostDoc post = new PostDoc();
        post.setUserId(userId);
        post.setTitle("测试帖子");
        post.setContent("Test Content");
        post.setStatus(1);
        post.setIsDeleted(0);
        post.setLikeCount(0);
        post.setCollectCount(0);
        post.setRatingAverage(0.0);
        post.setRatingCount(0);
        postRepository.save(post);
        this.postId = post.getId();
    }

    @AfterEach
    public void tearDown() {
        clearNoSqlData();
    }

    private void clearNoSqlData() {
        // 清理 Redis
        if (postId != null) {
            redisTemplate.delete(RedisKey.POST_LIKE_SET + postId);
            redisTemplate.delete(RedisKey.POST_COLLECT_SET + postId);
            redisTemplate.delete(RedisKey.POST_RATE_HASH + postId);
        }
        // 清理 Mongo
        postRepository.deleteAll();
        postLikeRepository.deleteAll();
        postCollectRepository.deleteAll();
        postRatingRepository.deleteAll();
        commentRepository.deleteAll();
        commentLikeRepository.deleteAll();

        // 不需要手动删 Postgres，@Transactional 会自动回滚本次插入的新用户
    }

    @Test
    public void testLikeAndUnlikePost() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("postId", postId);

        // 1. 点赞
        mockMvc.perform(post("/api/interaction/like/post")
                        .header("Authorization", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSONUtil.toJsonStr(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200));

        // 验证 Redis
        Boolean isMember = redisTemplate.opsForSet().isMember(RedisKey.POST_LIKE_SET + postId, userId.toString());
        Assertions.assertTrue(isMember, "Redis 中应当包含点赞记录");

        // 验证 Mongo (异步等待)
        Thread.sleep(1500);
        Assertions.assertTrue(postLikeRepository.existsByUserIdAndPostId(userId, postId), "Mongo应当有记录");

        PostDoc post = postRepository.findById(postId).orElseThrow();
        Assertions.assertEquals(1, post.getLikeCount(), "点赞数应当+1");

        // 2. 取消点赞
        mockMvc.perform(post("/api/interaction/unlike/post")
                        .header("Authorization", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSONUtil.toJsonStr(params)))
                .andExpect(status().isOk());

        // 验证回滚
        Assertions.assertFalse(redisTemplate.opsForSet().isMember(RedisKey.POST_LIKE_SET + postId, userId.toString()));
        Thread.sleep(1500);
        Assertions.assertFalse(postLikeRepository.existsByUserIdAndPostId(userId, postId));

        post = postRepository.findById(postId).orElseThrow();
        Assertions.assertEquals(0, post.getLikeCount(), "点赞数应当归零");
    }

    @Test
    public void testRatePost() throws Exception {
        PostRateDTO rateDTO = new PostRateDTO();
        rateDTO.setPostId(postId);
        rateDTO.setScore(4.0);

        // 1. 首次评分
        mockMvc.perform(post("/api/interaction/rate/post")
                        .header("Authorization", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSONUtil.toJsonStr(rateDTO)))
                .andExpect(status().isOk());

        Thread.sleep(1500);
        PostDoc post = postRepository.findById(postId).orElseThrow();
        Assertions.assertEquals(4.0, post.getRatingAverage());
        Assertions.assertEquals(1, post.getRatingCount());

        // 2. 修改评分
        rateDTO.setScore(5.0);
        mockMvc.perform(post("/api/interaction/rate/post")
                        .header("Authorization", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSONUtil.toJsonStr(rateDTO)))
                .andExpect(status().isOk());

        Thread.sleep(1500);
        post = postRepository.findById(postId).orElseThrow();
        Assertions.assertEquals(5.0, post.getRatingAverage());
        Assertions.assertEquals(1, post.getRatingCount()); // 人数不变

        // 验证 Redis
        Object score = redisTemplate.opsForHash().get(RedisKey.POST_RATE_HASH + postId, userId.toString());
        Assertions.assertEquals("5.0", score.toString());
    }

    @Test
    public void testCommentLike() throws Exception {
        CommentDoc comment = new CommentDoc();
        comment.setPostId(postId);
        comment.setUserId(userId);
        comment.setContent("Test");
        comment.setCreatedAt(LocalDateTime.now());
        comment.setLikeCount(0);
        commentRepository.save(comment);
        String commentId = comment.getId();

        Map<String, String> params = new HashMap<>();
        params.put("commentId", commentId);

        mockMvc.perform(post("/api/interaction/like/comment")
                        .header("Authorization", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSONUtil.toJsonStr(params)))
                .andExpect(status().isOk());

        Thread.sleep(1500);
        Assertions.assertTrue(commentLikeRepository.existsByUserIdAndCommentId(userId, commentId));
        CommentDoc updated = commentRepository.findById(commentId).orElseThrow();
        Assertions.assertEquals(1, updated.getLikeCount());
    }

    @Test
    public void testDuplicateLike() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("postId", postId);

        // 第一次
        mockMvc.perform(post("/api/interaction/like/post")
                .header("Authorization", token)
                .contentType(MediaType.APPLICATION_JSON)
                .content(JSONUtil.toJsonStr(params)));

        // 第二次
        mockMvc.perform(post("/api/interaction/like/post")
                .header("Authorization", token)
                .contentType(MediaType.APPLICATION_JSON)
                .content(JSONUtil.toJsonStr(params)));

        Thread.sleep(1500);
        PostDoc post = postRepository.findById(postId).orElseThrow();
        Assertions.assertEquals(1, post.getLikeCount(), "重复点赞不增加计数");
    }
}
