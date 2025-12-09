package com.ci.streams.service;

import com.ci.streams.util.Env;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 외부 API를 이용한 암호화 서비스. */
public class ApiCryptoService {

    private static final Logger LOG = LoggerFactory.getLogger(ApiCryptoService.class);
    private static final String ENC_URL = Env.must("API_ENC_URL");

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ApiTokenManager tokenManager;

    public ApiCryptoService() {
        this(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                new ObjectMapper(),
                ApiTokenManager.getInstance());
    }

    public ApiCryptoService(
            HttpClient httpClient, ObjectMapper objectMapper, ApiTokenManager tokenManager) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.tokenManager = tokenManager;
    }

    /**
     * 단건 암호화 처리.
     *
     * @param plainText 평문 텍스트
     * @return 암호화된 텍스트 or null (실패 시)
     */
    public String encrypt(String plainText) {
        return process(plainText, "enc");
    }

    /**
     * 단건 복호화 처리.
     *
     * @param cipherText 암호화된 텍스트
     * @return 복호화된 텍스트 or null (실패 시)
     */
    public String decrypt(String cipherText) {
        return process(cipherText, "dec");
    }

    private String process(String text, String type) {
        if (text == null || text.isEmpty()) {
            return null;
        }

        try {
            return processWithRetry(text, type, false);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Crypto operation ({}) failed for input: {}", type, text, e);
            }
            return null;
        }
    }

    private String processWithRetry(String text, String type, boolean isRetry) throws Exception {
        String token = tokenManager.getToken();

        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode inputArr = root.putArray("input");
        inputArr.add(text);
        root.put("type", type);

        String requestBody = objectMapper.writeValueAsString(root);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ENC_URL))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            JsonNode responseRoot = objectMapper.readTree(response.body());
            JsonNode outputNode = responseRoot.path("value");
            if (outputNode.isArray() && outputNode.size() > 0) {
                return outputNode.get(0).asText();
            }
            return null;
        } else if (response.statusCode() == 401 && !isRetry) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Got 401 Unauthorized. Retrying with fresh token...");
            }
            tokenManager.forceRefresh();
            return processWithRetry(text, type, true);
        } else {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Crypto API error. Status: {}, Body: {}", response.statusCode(), response.body());
            }
            throw new RuntimeException("API error: " + response.statusCode());
        }
    }
}
