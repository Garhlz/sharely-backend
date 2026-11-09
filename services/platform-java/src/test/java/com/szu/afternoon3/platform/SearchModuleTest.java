package com.szu.afternoon3.platform;
import org.junit.jupiter.api.Disabled;

import com.szu.afternoon3.platform.entity.es.PostEsDoc;
import com.szu.afternoon3.platform.repository.es.PostEsRepository;
import com.szu.afternoon3.platform.service.PostService;
import com.szu.afternoon3.platform.vo.PageResult;
import com.szu.afternoon3.platform.vo.PostVO;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@org.junit.jupiter.api.Tag("integration")
@SpringBootTest
@Disabled("旧 Java 搜索场景保留对照；对应关系与真实 ES 验证缺口见 docs/testing.md，禁止直接启用旧共享库夹具")
public class SearchModuleTest {

    @Autowired
    private PostService postService;

    @Autowired
    private PostEsRepository postEsRepository;

    /**
     * 测试前先准备一点假数据，确保测试环境有东西可查
     * 运行完你可以手动删掉，或者每次测试前都 deleteAll()
     */
    @BeforeEach
    public void setup() {
        // 如果你需要纯净环境，解开下面这行
        // postEsRepository.deleteAll();

        // 插入测试数据：深圳大学相关
        if (!postEsRepository.existsById("test-szu-1")) {
            PostEsDoc doc1 = new PostEsDoc();
            doc1.setId("test-szu-1");
            doc1.setTitle("深圳大学美食攻略");
            doc1.setTags(Arrays.asList("美食", "探店"));
            doc1.setContent("深大南区食堂很好吃");
            doc1.setLikeCount(100);
            doc1.setCreatedAt(LocalDateTime.now().minusDays(1)); // 昨天
            postEsRepository.save(doc1);
        }

        if (!postEsRepository.existsById("test-szu-2")) {
            PostEsDoc doc2 = new PostEsDoc();
            doc2.setId("test-szu-2");
            doc2.setTitle("带你逛逛深大"); // 注意：标题没有"深圳"，看能否通过拼音搜到"深"
            doc2.setTags(Arrays.asList("校园", "深圳")); // 标签有"深圳"
            doc2.setContent("荔园很美");
            doc2.setLikeCount(5);
            doc2.setCreatedAt(LocalDateTime.now()); // 今天 (最新)
            postEsRepository.save(doc2);
        }
        
        // 给 ES 一点时间建立索引 (1秒)
        try { Thread.sleep(1000); } catch (InterruptedException e) {}
    }

    /**
     * 测试场景 1: 拼音补全 (Suggest)
     * 目标: 输入 "sz" 能补全出 "深圳..." 且带高亮
     */
    @Test
    public void testSuggestionWithPinyin() {
        String keyword = "sz";
        log.info("测试补全，输入关键词: {}", keyword);

        List<String> suggestions = postService.getSearchSuggestions(keyword);
        
        log.info("补全结果: {}", suggestions);

        // 断言 1: 结果不为空
        assertTrue(suggestions.size() > 0, "补全结果不应为空");
        
        // 断言 2: 第一条应该是用户输入的 keyword (我们在代码里手动 add 的)
        assertEquals(keyword, suggestions.get(0));

        // 断言 3: 应该包含高亮标签 <em>
        // 注意：如果你搜 "sz"，匹配到 "深圳"，ES 高亮会返回 "<em>深</em><em>圳</em>..."
        boolean hasHighlight = suggestions.stream().anyMatch(s -> s.contains("<em>"));
        assertTrue(hasHighlight, "结果中应该包含高亮标签 <em>");
    }

    /**
     * 测试场景 2: 搜索排序 (Sorting)
     * 目标: 验证 "new" (最新) 和 "hot" (点赞/综合) 的顺序不同
     */
    @Test
    public void testSearchSorting() {
        String keyword = "深圳"; // 我们的测试数据都包含这个意图

        // A. 测试按最新排序 (new)
        PageResult<PostVO> newResult = postService.searchPosts(keyword,null, 1, 10, "new");
        List<PostVO> newRecords = newResult.getRecords();
        
        log.info("按最新排序第一条: {}", newRecords.get(0).getTitle());
        // 预期: test-szu-2 (今天发的) 排在 test-szu-1 (昨天发的) 前面
        assertEquals("test-szu-2", newRecords.get(0).getId());


        // B. 测试按点赞/热度排序 (likes 或 hot)
        PageResult<PostVO> hotResult = postService.searchPosts(keyword, null,1, 10, "likes");
        List<PostVO> hotRecords = hotResult.getRecords();
        
        log.info("按点赞排序第一条: {}", hotRecords.get(0).getTitle());
        // 预期: test-szu-1 (100赞) 排在 test-szu-2 (5赞) 前面
        assertEquals("test-szu-1", hotRecords.get(0).getId());
    }

    /**
     * 测试场景 3: 字段过滤 (Source Filter)
     * 目标: 确保搜索结果里 content 字段为空字符串 (省流量)
     */
    @Test
    public void testSearchContentFilter() {
        PageResult<PostVO> result = postService.searchPosts("美食",null, 1, 10, "new");
        List<PostVO> records = result.getRecords();

        if (records.size() > 0) {
            String content = (String) records.get(0).getContent();
            log.info("搜索结果中的 content 字段: '{}'", content);
            assertEquals("", content, "搜索结果中 content 字段应该被清空");
        }
    }
}
