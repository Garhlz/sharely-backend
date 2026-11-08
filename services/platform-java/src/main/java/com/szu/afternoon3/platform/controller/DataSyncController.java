package com.szu.afternoon3.platform.controller;

import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

@RestController
@RequestMapping("/admin/sync")
public class DataSyncController {
    @Value("${app.admin.secret:szu123}")
    private String adminSecret;
    @Value("${sync.sidecar.url:http://localhost:18088}")
    private String sidecarUrl;
    @Value("${sync.sidecar.token:${app.admin.secret:szu123}}")
    private String sidecarToken;

    /** 保留后台入口，由 Sidecar 统一执行索引重建。 */
    @PostMapping("/mongo-to-es")
    public Map<?, ?> syncAllMongoToEs(@RequestHeader(value = "X-Admin-Token", required = false) String token) {
        if (token == null || !token.equals(adminSecret)) {
            return Map.of("code", 403, "message", "无权操作，Token 错误");
        }
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(5));
        factory.setReadTimeout(Duration.ofMinutes(11));
        return RestClient.builder().requestFactory(factory).build()
                .post().uri(sidecarUrl + "/admin/reindex/posts")
                .header("X-Admin-Token", sidecarToken)
                .retrieve().body(Map.class);
    }
}
