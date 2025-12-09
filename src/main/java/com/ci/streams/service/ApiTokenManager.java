package com.ci.streams.service;

import com.ci.streams.util.Env;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** API Bearer 토큰 관리자 (싱글톤). 토큰 발급, 갱신 및 유효성 관리를 담당합니다. */
public class ApiTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(ApiTokenManager.class);
    private static final ApiTokenManager INSTANCE = new ApiTokenManager();

    private static final String AUTH_URL = Env.must("API_AUTH_URL");
    private static final String USERNAME = Env.must("API_USERNAME");
    private static final String PASSWORD = Env.must("API_PASSWORD");

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Lock lock = new ReentrantLock();

    private volatile String accessToken;
    private volatile Instant expiresAt;

    private ApiTokenManager() {
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.objectMapper = new ObjectMapper();
        this.expiresAt = Instant.MIN;
    }

    public static ApiTokenManager getInstance() {
        return INSTANCE;
    }

    /** 유효한 토큰을 반환합니다. 만료되었거나 없으면 새로 발급받습니다. */
    public String getToken() {
        if (isValid()) {
            return accessToken;
        }

        lock.lock();
        try {
            // Double-checked locking
            if (isValid()) {
                return accessToken;
            }
            refreshToken();
            return accessToken;
        } finally {
            lock.unlock();
        }
    }

    /** 토큰을 강제로 갱신합니다 (e.g. 401 발생 시). */
    public void forceRefresh() {
        lock.lock();
        try {
            refreshToken();
        } finally {
            lock.unlock();
        }
    }

    private boolean isValid() {
        // 만료 1분 전까지 유효한 것으로 간주
        return accessToken != null && Instant.now().isBefore(expiresAt.minusSeconds(60));
    }

    private void refreshToken() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Refreshing API Token...");
        }
        try {
            Map<String, String> bodyMap = Map.of(
                    "username", USERNAME,
                    "password", PASSWORD);
            String requestBody = objectMapper.writeValueAsString(bodyMap);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(AUTH_URL))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode root = objectMapper.readTree(response.body());
                String token = root.path("value").asText(null);
                if (token != null) {
                    this.accessToken = token;
                    // 토큰 유효기간 24시간 -> 23시간으로 보수적 설정
                    this.expiresAt = Instant.now().plus(Duration.ofHours(23));
                    if (LOG.isInfoEnabled()) {
                        LOG.info("API Token refreshed successfully. Expires at: {}", expiresAt);
                    }
                } else {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Failed to parse token from response: {}", response.body());
                    }
                    throw new RuntimeException("Token parsing failed");
                }
            } else {
                if (LOG.isErrorEnabled()) {
                    LOG.error(
                            "Authentication failed. Status: {}, Body: {}",
                            response.statusCode(),
                            response.body());
                }
                throw new RuntimeException("Authentication failed with status " + response.statusCode());
            }

        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Error refreshing token", e);
            }
            throw new RuntimeException("Failed to refresh token", e);
        }
    }
}
