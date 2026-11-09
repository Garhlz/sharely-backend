package com.szu.afternoon3.platform.repository.es;

import com.szu.afternoon3.platform.entity.es.PostEsDoc;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PostEsRepository extends ElasticsearchRepository<PostEsDoc, String> {
    // 基础的 CRUD 已经内置了
    // 复杂的搜索我们在 Service 里用 Template 写
}
