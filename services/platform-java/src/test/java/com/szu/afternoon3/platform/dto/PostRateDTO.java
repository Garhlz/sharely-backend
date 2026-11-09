package com.szu.afternoon3.platform.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
/** 仅供迁移前禁用用例编译，不属于当前 Java 业务契约。 */
public class PostRateDTO {
    @NotBlank(message = "帖子ID不能为空")
    private String postId;

    @NotNull(message = "评分不能为空")
    @DecimalMin(value = "0.0", message = "评分不能低于0分")
    @DecimalMax(value = "5.0", message = "评分不能高于5分")
    private Double score;
}
