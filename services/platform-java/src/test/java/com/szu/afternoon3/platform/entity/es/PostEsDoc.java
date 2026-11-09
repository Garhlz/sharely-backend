package com.szu.afternoon3.platform.entity.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;
import org.springframework.data.elasticsearch.core.suggest.Completion;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Document(indexName = "posts") // 对应 ES 中的索引名称
@Setting(settingPath = "es-settings.json")
public class PostEsDoc {

    @Id
    private String id; // 对应 Mongo 的 ID

    @Field(type = FieldType.Long)
    private Long userId;

    private String userNickname;
    private String userAvatar;

    // 标题：存储时最细粒度分词，搜索时智能分词
    @MultiField(
            mainField = @Field(type = FieldType.Text, analyzer = "ik_max_word", searchAnalyzer = "ik_smart"),
            otherFields = {
                    @InnerField(suffix = "pinyin", type = FieldType.Text, analyzer = "ik_pinyin_analyzer", searchAnalyzer = "ik_pinyin_analyzer")
            }
    )
    private String title;

    @MultiField(
            mainField = @Field(type = FieldType.Text, analyzer = "ik_max_word", searchAnalyzer = "ik_smart"),
            otherFields = {
                    @InnerField(suffix = "pinyin", type = FieldType.Text, analyzer = "ik_pinyin_analyzer", searchAnalyzer = "ik_pinyin_analyzer"),
                    @InnerField(suffix = "keyword", type = FieldType.Keyword)
            }
    )
    private List<String> tags;

    // 内容：同上
    @Field(type = FieldType.Text, analyzer = "ik_max_word", searchAnalyzer = "ik_smart")
    private String content;


    // --- 以下字段主要用于结果展示和排序，不一定用于全文检索 ---

    @Field(type = FieldType.Keyword, index = false) // index=false 表示不对该字段建索引，只存储(节省空间)
    private String cover; 

    @Field(type = FieldType.Integer)
    private Integer type; // 0:图文, 1:视频

    @Field(type = FieldType.Integer)
    private Integer coverWidth;
    
    @Field(type = FieldType.Integer)
    private Integer coverHeight;

    // --- 排序因子 ---
    
    @Field(type = FieldType.Integer)
    private Integer likeCount;

    @Field(type = FieldType.Date, format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime createdAt;
}
