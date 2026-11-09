package com.szu.afternoon3.platform.controller;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.crypto.digest.BCrypt;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.szu.afternoon3.platform.common.Result;
import com.szu.afternoon3.platform.common.UserContext;
import com.szu.afternoon3.platform.dto.CommentCreateDTO;
import com.szu.afternoon3.platform.dto.PostCreateDTO;
import com.szu.afternoon3.platform.entity.User;
import com.szu.afternoon3.platform.entity.mongo.PostDoc; // 假设你的MongoDB实体类位置
import com.szu.afternoon3.platform.mapper.UserMapper;
import com.szu.afternoon3.platform.grpc.CommentRpcClient;
import com.szu.afternoon3.platform.service.PostService;
import com.szu.afternoon3.platform.service.UserService;
import com.szu.afternoon3.platform.grpc.NotificationRpcClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 开发环境数据填充工具
 * 访问地址: POST /api/dev/seed
 */
@RestController
@RequestMapping("/api/dev")
@Slf4j
public class DevDataController {

    @Autowired private UserService userService;
    @Autowired private PostService postService;
    @Autowired private CommentRpcClient commentRpcClient;
    @Autowired private UserMapper userMapper;
    @Autowired private MongoTemplate mongoTemplate; // 引入 MongoTemplate 用于反查帖子ID

    // 1. 用户画像
    private static final List<String[]> MOCK_USERS = Arrays.asList(
            new String[]{"TechGeek", "tech@test.com", "热爱代码，探索前沿科技"},
            new String[]{"PhotoLife", "photo@test.com", "用镜头记录生活的美好"},
            new String[]{"FoodieJane", "jane@test.com", "探店达人 | 寻找城市角落的美味"},
            new String[]{"StudentMike", "mike@test.com", "大三学生，正在准备考研"},
            new String[]{"CatLover", "cat@test.com", "猫奴一枚，只有猫猫能治愈我"}
    );

    // 2. 帖子模板
    private static final List<PostTemplate> POST_TEMPLATES = Arrays.asList(
            new PostTemplate("Java 21 新特性尝鲜", "最近试了一下 Java 21 的虚拟线程，性能提升真的很明显！强烈推荐大家升级。", 2, null),
            new PostTemplate("周末去海边采风", "天气真好，随手拍了几张。大海真的很治愈。", 0, "https://images.unsplash.com/photo-1507525428034-b723cf961d3e"),
            new PostTemplate("学校食堂的新菜品", "今天食堂二楼开了家新窗口，味道意外的不错，价格也实惠。", 0, "https://images.unsplash.com/photo-1504674900247-0877df9cc836"),
            new PostTemplate("推荐一本好书《深度工作》", "在这个碎片化的时代，保持专注太难了。这本书给了我很多启发。", 2, null),
            new PostTemplate("深夜加班的快乐", "改完最后一个 Bug，看着窗外的夜景，感觉一切都值得。", 0, "https://images.unsplash.com/photo-1497366216548-37526070297c"),
            new PostTemplate("求助：Spring Boot 启动报错", "有没有大佬遇到过这个 BeanCreationException？卡了一下午了...", 2, null),
            new PostTemplate("新入手的机械键盘", "红轴的手感果然不一样，打字停不下来。", 0, "https://images.unsplash.com/photo-1587829741301-dc798b91a603")
    );

    // 3. 评论库
    private static final List<String> COMMENTS = Arrays.asList(
            "太棒了，学到了！", "图拍得真好看！", "感同身受...", "大佬求带！",
            "这个我也遇到过，检查下配置文件。", "非常有用的分享，感谢。",
            "哇，看起来好好吃！", "支持一下！", "这就是生活的意义啊。", "狠狠羡慕了。"
    );

    @PostMapping("/seed")
    @Transactional(rollbackFor = Exception.class)
    public Result<String> seedData() {
        log.info(">>> 开始生成全套测试数据 (用户 -> 关注 -> 发帖 -> 评论)...");
        List<User> createdUsers = new ArrayList<>();

        // --- Step 1: 创建/获取用户 ---
        for (String[] u : MOCK_USERS) {
            String nickname = u[0];
            String email = u[1];
            String bio = u[2];

            User user = userMapper.selectOne(new LambdaQueryWrapper<User>().eq(User::getEmail, email));
            if (user == null) {
                user = new User();
                user.setEmail(email);
                user.setNickname(nickname);
                user.setBio(bio);
                user.setPassword(BCrypt.hashpw("123456"));
                user.setRole("USER");
                user.setStatus(1);
                user.setAvatar("https://api.dicebear.com/7.x/avataaars/svg?seed=" + nickname);
                userMapper.insert(user);
                log.info("创建用户: {}", nickname);
            }
            createdUsers.add(user);
        }

        // --- Step 2: 随机互相关注 ---
        log.info(">>> 正在建立社交关系...");
        for (User currentUser : createdUsers) {
            simulateLogin(currentUser);
            try {
                // 每个用户随机关注 1-3 个其他用户
                List<User> targets = RandomUtil.randomEleList(createdUsers, RandomUtil.randomInt(1, 4));
                for (User target : targets) {
                    if (!target.getId().equals(currentUser.getId())) { // 不能关注自己
                        try {
                            // 这里可能会抛出"已关注"异常，捕获忽略即可
                            userService.followUser(target.getId().toString());
                        } catch (Exception ignored) {}
                    }
                }
            } finally {
                UserContext.clear();
            }
        }

        // 3. 发帖 (核心修改点)
        log.info(">>> 正在生成帖子...");
        for (User user : createdUsers) {
            simulateLogin(user);
            try {
                int postCount = 2 + RandomUtil.randomInt(3);
                for (int j = 0; j < postCount; j++) {
                    PostTemplate tpl = RandomUtil.randomEle(POST_TEMPLATES);
                    PostCreateDTO postDto = new PostCreateDTO();
                    postDto.setTitle(tpl.title + " " + RandomUtil.randomString(3));
                    postDto.setContent(tpl.content);

                    // 【修正点】在这里处理类型和图片
                    if (tpl.image != null) {
                        // 有图 -> Type=0
                        postDto.setType(0);
                        postDto.setImages(Arrays.asList(tpl.image));
                    } else {
                        // 无图 -> Type=2 (纯文)
                        // 建议：去修改你的 PostService，去掉對 Type=2 的图片强校验
                        // 如果你实在不想改 Service，这里就只能假装成 Type=0 并塞一张默认图
                         postDto.setType(0);
                         postDto.setImages(Collections.singletonList("https://afternoon3-rednote.oss-cn-shenzhen.aliyuncs.com/default_avatar.jpg"));

//                        // 正常逻辑应该是这样：
//                        postDto.setType(2);
//                        postDto.setImages(Collections.emptyList()); // 显式设置空列表，避免 null
                    }

                    postDto.setTags(Arrays.asList("测试", "生活", "技术"));

                    try {
                        postService.createPost(postDto);
                    } catch (Exception e) {
                        log.error("发帖失败: title={}, type={}, err={}", postDto.getTitle(), postDto.getType(), e.getMessage());
                        // 继续下一个循环，不要中断整个流程
                    }

                    try { Thread.sleep(50); } catch (InterruptedException e) {}
                }
            } finally { UserContext.clear(); }
        }

        // --- Step 4: 批量获取刚刚生成的帖子 ID ---
        // 技巧：直接查这些测试用户的帖子
        List<Long> userIds = createdUsers.stream().map(User::getId).collect(Collectors.toList());
        Query query = new Query(Criteria.where("userId").in(userIds));
        // 限制只取最近的 50 条，避免全表扫描
        query.limit(50).with(org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC, "createdAt"));
        List<PostDoc> recentPosts = mongoTemplate.find(query, PostDoc.class);

        if (CollUtil.isEmpty(recentPosts)) {
            return Result.success("用户生成成功，但帖子生成似乎有问题，未找到帖子。");
        }

        // --- Step 5: 生成评论；点赞通过网关/Go 冒烟生成，不再调用旧 Java 空实现 ---
        log.info(">>> 正在生成评论（点赞需通过 Go 网关生成）...");
        for (PostDoc post : recentPosts) {
            // 对于每个帖子，随机抽取 2-4 个用户进行互动
            List<User> interactUsers = RandomUtil.randomEleList(createdUsers, RandomUtil.randomInt(2, 5));

            for (User u : interactUsers) {
                simulateLogin(u);
                try {
                    // 评论 (40% 概率)
                    if (RandomUtil.randomInt(100) < 40) {
                        CommentCreateDTO commentDto = new CommentCreateDTO();
                        commentDto.setPostId(post.getId());
                        commentDto.setContent(RandomUtil.randomEle(COMMENTS));
                        commentRpcClient.createComment(commentDto);
                    }
                } finally {
                    UserContext.clear();
                }
            }
        }

        return Result.success(String.format("生成完毕！用户数:%d, 帖子数(本次扫描):%d. 已尝试生成关注与评论；点赞请通过网关生成。", createdUsers.size(), recentPosts.size()));
    }

    // 辅助方法：模拟登录上下文
    private void simulateLogin(User user) {
        UserContext.setUserId(user.getId());
        UserContext.setRole(user.getRole());
    }

    // 内部类：帖子模板
    static class PostTemplate {
        String title;
        String content;
        int type;
        String image;
        public PostTemplate(String t, String c, int type, String i) {
            this.title = t; this.content = c; this.type = type; this.image = i;
        }
    }

    @Autowired
    private NotificationRpcClient notificationRpcClient;

    @PostMapping("/clean-notifications")
    @Transactional(rollbackFor = Exception.class)
    public Result<String> cleanNotifications() {
        long count = notificationRpcClient.cleanDuplicateNotifications();
        return Result.success("清洗成功，删除了 " + count + " 条重复数据");
    }
}
