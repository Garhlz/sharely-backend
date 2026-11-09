package com.szu.afternoon3.platform;

import cn.hutool.core.collection.CollUtil;
import com.szu.afternoon3.platform.entity.es.PostEsDoc;
import com.szu.afternoon3.platform.entity.mongo.PostDoc;
import com.szu.afternoon3.platform.repository.PostRepository;
import com.szu.afternoon3.platform.repository.es.PostEsRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@org.junit.jupiter.api.Tag("integration")
@SpringBootTest // 启动 Spring 上下文，获取 Repository
@Disabled("旧手工 ES 重建工具保留参考；当前重建由 Sidecar 管理接口执行，禁止 integration 测试清空共享索引")
public class DataSyncTest {

    @Autowired
    private PostRepository postRepository; // Mongo

    @Autowired
    private PostEsRepository postEsRepository; // ES

    @Test
    public void syncAllMongoToEs() {
        log.info("========== 开始全量同步 Mongo -> ES ==========");

        // 1. (可选) 清空旧数据，防止重复
         postEsRepository.deleteAll();
         log.info("旧索引数据已清空");

        int page = 0;
        int size = 100; // 每次处理 100 条，防止内存溢出
        int totalSynced = 0;

        while (true) {
            // 分页查询 Mongo 中未删除且已发布的帖子
            Page<PostDoc> mongoPage = postRepository.findByStatusAndIsDeleted(1, 0, PageRequest.of(page, size));
            List<PostDoc> mongoDocs = mongoPage.getContent();

            if (CollUtil.isEmpty(mongoDocs)) {
                break;
            }

            List<PostEsDoc> esDocs = new ArrayList<>();
            for (PostDoc mongoDoc : mongoDocs) {
                PostEsDoc esDoc = new PostEsDoc();
                
                // A. 基础属性拷贝
                BeanUtils.copyProperties(mongoDoc, esDoc);

                // B. 处理名字不一致的字段 (Mongo: userNickname -> ES: userNickname)
                // 你的代码里 Mongo和ES字段名应该是一样的，如果不一样在这里手动 set
                // esDoc.setUserNickname(mongoDoc.getUserNickname());

                // C. 确保非空处理
                if (esDoc.getLikeCount() == null) esDoc.setLikeCount(0);
                if (esDoc.getCreatedAt() == null) esDoc.setCreatedAt(java.time.LocalDateTime.now());
                // 显式检查并确保 createdAt 包含时分秒
                if (mongoDoc.getCreatedAt() != null) {
                    esDoc.setCreatedAt(mongoDoc.getCreatedAt());
                } else {
                    esDoc.setCreatedAt(LocalDateTime.now());
                }
                // 注意：这里不需要手动构建 suggestion，ES 的 analyzer 会自动处理 title/tags
                
                esDocs.add(esDoc);
            }

            // 批量保存到 ES
            postEsRepository.saveAll(esDocs);
            
            totalSynced += esDocs.size();
            log.info("已同步第 {} 页，本页 {} 条，总计 {} 条", page + 1, esDocs.size(), totalSynced);

            if (!mongoPage.hasNext()) {
                break;
            }
            page++;
        }

        log.info("========== 同步完成，共计 {} 条数据 ==========", totalSynced);
    }
}
