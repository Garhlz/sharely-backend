package com.szu.afternoon3.platform.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.alibaba.dashscope.aigc.multimodalconversation.MultiModalConversation;
import com.alibaba.dashscope.aigc.multimodalconversation.MultiModalConversationParam;
import com.alibaba.dashscope.aigc.multimodalconversation.MultiModalConversationResult;
import com.alibaba.dashscope.common.Message;
import com.alibaba.dashscope.common.MultiModalMessage;
import com.alibaba.dashscope.common.Role;
import com.szu.afternoon3.platform.entity.mongo.PostDoc;
import com.szu.afternoon3.platform.service.AiService;
import com.szu.afternoon3.platform.vo.AiAuditResultVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class AiServiceImpl implements AiService {

    @Value("${ai.enabled:true}")
    private boolean enabled;

    @Value("${ai.dashscope.api-key}")
    private String apiKey;

    // 直接使用qwen-vl-plus
    @Value("${ai.dashscope.model:qwen-vl-plus}")
    private String modelName;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final int LIMIT_SUMMARY_PER_DAY = 5; // 每天每人最多总结 5 次
    private static final int LIMIT_REPLY_PER_DAY = 10;   // 每天每人最多对话 10 次

    /**
     * 检查是否超过限额
     * @param type 业务类型 (summary / reply)
     * @param maxLimit 最大次数
     * @return true=通过, false=已超额
     */
    private boolean checkQuota(Long userId, String type, int maxLimit) {
        if (userId == null) return true; // 系统触发或未登录，视情况而定，这里默认放行或直接拒接

        // Key 格式: ai:quota:summary:1001:2025-12-28
        String dateStr = DateUtil.today(); // Hutool 获取 yyyy-MM-dd
        String key = "ai:quota:" + type + ":" + userId + ":" + dateStr;

        // Redis 原子递增
        Long count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            // 如果是第一次访问，设置 24 小时过期 (其实设到当天23:59:59最精准，但24h够用了)
            redisTemplate.expire(key, 24, TimeUnit.HOURS);
        }

        if (count != null && count > maxLimit) {
            log.warn("用户 {} 的 AI {} 额度已耗尽 (今日第 {} 次)", userId, type, count);
            return false;
        }
        return true;
    }

    /**
     * 通用 Qwen-VL 调用方法 (支持多模态)
     * @param systemPrompt 系统人设
     * @param userText 用户纯文本内容
     * @param images 图片URL列表 (List<String>)
     * @param video 视频URL (单个 String)
     * @param temperature 随机性 (0.0 - 2.0)
     */
    private String callQwenVL(String systemPrompt, String userText, List<String> images, String video, double temperature) {
        if (!enabled) return null;
        long startTime = System.currentTimeMillis();
        String apiCallName = "QwenVL_Call";

        try {
            MultiModalConversation conversation = new MultiModalConversation();

            // 1. 构建 System Message (人设)
            MultiModalMessage systemMessage = MultiModalMessage.builder()
                    .role(Role.SYSTEM.getValue())
                    .content(List.of(Map.of("text", systemPrompt)))
                    .build();

            // 2. 构建 User Content (混合图、文、视频)
            List<Map<String, Object>> contentList = new ArrayList<>();

            // 2.1 添加视频 (如果有) - Qwen-VL 只能处理一个视频
            if (StrUtil.isNotBlank(video)) {
                contentList.add(Map.of("video", video));
                contentList.add(Map.of("text", "（用户上传了一段视频）\n"));
            }

            // 2.2 添加图片 (支持多张 List<String>)
            if (CollUtil.isNotEmpty(images)) {
                for (String imgUrl : images) {
                    if (StrUtil.isNotBlank(imgUrl)) {
                        contentList.add(Map.of("image", imgUrl.trim()));
                    }
                }
            }

            // 2.3 添加文本
            if (StrUtil.isNotBlank(userText)) {
                contentList.add(Map.of("text", userText));
            }

            // 2.4 组装 User Message
            MultiModalMessage userMessage = MultiModalMessage.builder()
                    .role(Role.USER.getValue())
                    .content(contentList)
                    .build();

            // 3. 构建参数
            MultiModalConversationParam param = MultiModalConversationParam.builder()
                    .model(modelName)
                    .apiKey(apiKey)
                    .messages(Arrays.asList(systemMessage, userMessage))
                    // topP 控制生成多样性
                    .topP(0.8)
                    .build();

            // 4. 发起调用
            MultiModalConversationResult result = conversation.call(param);

            // 5. 解析结果
            String content = result.getOutput().getChoices().get(0).getMessage().getContent().get(0).get("text").toString();

            long duration = System.currentTimeMillis() - startTime;
            log.info("[{}] 调用成功，耗时: {} ms", apiCallName, duration);

            return content;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("[{}] 调用异常，耗时: {} ms", apiCallName, duration, e);
            return null;
        }
    }

    /**
     * 场景 1: 生成标签
     * 传入图片后，AI 可以根据图片内容生成标签
     */
    @Override
    public List<String> generateTags(String title, String content, List<String> images, String video) {
        // 如果没有任何内容，直接返回
        if (StrUtil.isBlank(content) && CollUtil.isEmpty(images) && StrUtil.isBlank(video)) {
            return Collections.emptyList();
        }

        String systemPrompt = """
            你是一个社交平台助手。请分析用户提供的【文本、图片或视频】。
            任务：生成 3-5 个热门标签。
            要求：
            1. 必须根据视觉内容（如有）和文本内容共同判断。
            2. 直接返回 JSON 字符串数组，例如：["美食", "探店", "火锅"]
            3. 严禁包含 Markdown 格式（不要 ```json），严禁包含任何解释语。
            4. 标签简短有力，不带 # 号。
            """;

        String input = String.format("标题：%s\n内容：%s", StrUtil.nullToEmpty(title), StrUtil.nullToEmpty(content));

        if (images != null && !images.isEmpty()) {
            images = new ArrayList<>(images.stream().limit(4).toList());
        }

        String response = callQwenVL(systemPrompt, input, images, video, 0.5);

        if (StrUtil.isBlank(response)) return new ArrayList<>();

        try {
            // 清洗数据
            String cleanJson = response.replace("```json", "").replace("```", "").trim();
            JSONArray array = JSONUtil.parseArray(cleanJson);
            return array.toList(String.class);
        } catch (Exception e) {
            log.warn("AI 标签解析失败: {}", response);
            return new ArrayList<>();
        }
    }

    /**
     * 场景 2: 帖子智能总结
     */
    @Override
    public String generatePostSummary(Long userId, String title, String content, List<String> images, String video) {
        // 内容太少且没图没视频，不总结
        if (StrUtil.length(content) < 10 && CollUtil.isEmpty(images) && StrUtil.isBlank(video)) {
            return null;
        }

        // 2. 【新增】限流检查
        if (!checkQuota(userId, "summary", LIMIT_SUMMARY_PER_DAY)) {
            // 超额了，直接不生成总结
            return null;
        }

        String input = String.format("标题：%s\n内容：%s", StrUtil.nullToEmpty(title), StrUtil.nullToEmpty(content));

        String systemPrompt = """
            你是一个社交平台的“课代表”机器人。
            任务：阅读用户的帖子（包含文字、图片或视频），用幽默、简短的语言进行总结（神总结/TL;DR）。
            要求：
            1. 必须结合视觉信息！例如：如果图片是猫，你要提到猫；如果视频是跳舞，你要提到跳舞。
            2. 必须以第一人称评论的口吻，不要像机器摘要。
            3. 字数控制在 60 字以内。
            4. 开头可以是“课代表总结：”或者“省流：”。
            """;

        if (images != null && !images.isEmpty()) {
            images = new ArrayList<>(images.stream().limit(4).toList());
        }
        return callQwenVL(systemPrompt, input, images, video, 1.2);
    }

    /**
     * 场景 3: 评论区交互式回复
     */
    @Override
    public String generateInteractiveReply(Long userId, String postTitle, String postContent, List<String> postImages, String postVideo,
                                           String parentContent, String userPrompt) {

        // 1. 【新增】限流检查
        if (!checkQuota(userId, "reply", LIMIT_REPLY_PER_DAY)) {
            // 超额了，返回特定文案告诉用户
            return "（小映累了，今日回复额度已用完，明天再来找我玩吧💤）";
        }

        String systemPrompt = """
            你是一个社交平台的高情商AI助手，名字叫"小映"。
            用户在评论区 @了你，你需要根据帖子内容（含视觉内容）回答用户的问题。
            要求：
            1. 语气亲切、活泼，像个真实的朋友。
            2. 深度结合图片/视频内容！例如：看到美食可以说"看起来好好吃"，看到风景可以说"这里是哪里呀"。
            3. 回复尽量简短（80字以内）。
            4. 如果用户是在闲聊，就幽默回应。
            """;

        // 构建上下文
        StringBuilder inputBuilder = new StringBuilder();
        inputBuilder.append("【帖子标题】").append(StrUtil.nullToEmpty(postTitle)).append("\n");
        inputBuilder.append("【帖子文案】").append(StrUtil.subPre(postContent, 500)).append("\n");

        if (StrUtil.isNotBlank(parentContent)) {
            inputBuilder.append("【回复的目标评论】").append(parentContent).append("\n");
        }

        inputBuilder.append("【用户对你说】").append(userPrompt);

        List<String> images = null;
        if (postImages != null && !postImages.isEmpty()) {
            images = new ArrayList<>(postImages.stream().limit(4).toList());
        }


        // 将帖子的图片/视频传给 AI
        return callQwenVL(systemPrompt, inputBuilder.toString(), images, postVideo, 1.3);
    }

    /**
     * 场景 4: 内容安全审核
     * 复用 callQwenVL 方法，实现统一调用
     */
    @Override
    public AiAuditResultVO auditPostContent(PostDoc post) {
        // 1. 构造系统提示词 (System Prompt) - 严格审核员模式
        String systemPrompt = """
            你是一个严格的社区内容安全审核员。请分析用户提交的帖子内容（包含文本和图片）。
            检测维度包括：色情低俗、血腥暴力、政治敏感、恶意广告、人身攻击。
            
            请严格按照以下 JSON 格式返回结果（不要包含 Markdown 代码块，只返回纯 JSON 字符串）：
            {
                "conclusion": "PASS" | "BLOCK" | "REVIEW",
                "riskType": "色情" | "暴力" | "政治" | "广告" | "谩骂" | "无",
                "confidence": 0.95,
                "suggestion": "具体的审核意见和原因描述"
            }
            注意：如果是视频贴，请重点审核封面图和标题文本。
            """;

        // 2. 构造用户文本输入
        String userText = String.format("【标题】：%s\n【内容】：%s",
                StrUtil.nullToEmpty(post.getTitle()),
                StrUtil.nullToEmpty(post.getContent()));

        // 3. 准备图片/视频素材
        // 策略：为了审核速度（Video Token 很贵且慢），对于视频贴，我们优先审核“封面图 + 标题”。
        // 只有图文贴才传所有图片（限制前 4 张）。
        List<String> images = new ArrayList<>();
        String video = null;

        if (post.getType() != null && post.getType() == 1) {
            // === 视频贴处理 ===
            // 方案 A (快速省钱): 只审封面。通常封面和标题能暴露大部分违规。
            if (StrUtil.isNotBlank(post.getCover())) {
                images.add(post.getCover());
            }
            // 方案 B (深度审核): 如果你需要审视频内容，就把下一行注释打开，并注释掉上面的 images.add
            // if (CollUtil.isNotEmpty(post.getResources())) video = post.getResources().get(0);
        } else {
            // === 图文贴处理 ===
            if (CollUtil.isNotEmpty(post.getResources())) {
                // 限制前 4 张，防止 Token 超限或超时
                images.addAll(post.getResources().stream().limit(4).toList());
            }
        }

        // 4. 复用 callQwenVL 发起调用
        // temperature 设置为 0.1，让审核结果尽可能稳定，不要发散
        String jsonResult = callQwenVL(systemPrompt, userText, images, video, 0.1);

        // 5. 结果处理与兜底
        if (StrUtil.isBlank(jsonResult)) {
            return AiAuditResultVO.builder()
                    .conclusion("REVIEW")
                    .riskType("服务异常")
                    .confidence(0.0)
                    .suggestion("AI 服务未响应，请转人工审核")
                    .build();
        }

        try {
            // 6. JSON 清洗与反序列化
            String cleanJson = cleanJsonStr(jsonResult);
            return JSONUtil.toBean(cleanJson, AiAuditResultVO.class);
        } catch (Exception e) {
            log.error("[AI Audit] 结果解析失败, 原始内容: {}", jsonResult, e);
            return AiAuditResultVO.builder()
                    .conclusion("REVIEW")
                    .riskType("解析错误")
                    .confidence(0.0)
                    .suggestion("AI 返回格式异常，需人工复核")
                    .build();
        }
    }

    /**
     * 辅助方法：清洗 JSON 字符串 (去除 markdown 代码块标记)
     */
    private String cleanJsonStr(String raw) {
        if (StrUtil.isBlank(raw)) return "{}";
        String clean = raw.trim();
        // 去除 ```json 和 ``` 包裹
        if (clean.startsWith("```json")) {
            clean = clean.substring(7);
        } else if (clean.startsWith("```")) {
            clean = clean.substring(3);
        }

        if (clean.endsWith("```")) {
            clean = clean.substring(0, clean.length() - 3);
        }
        return clean.trim();
    }
}
