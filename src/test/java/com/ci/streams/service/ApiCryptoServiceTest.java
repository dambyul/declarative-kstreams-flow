package com.ci.streams.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiCryptoServiceTest {

  static {
    System.setProperty("API_ENC_URL", "http://localhost/enc");
    System.setProperty("API_AUTH_URL", "http://localhost/auth");
    System.setProperty("API_USERNAME", "testuser");
    System.setProperty("API_PASSWORD", "testpass");
  }

  private HttpClient httpClient;
  private ObjectMapper objectMapper;
  private ApiTokenManager tokenManager;
  private ApiCryptoService cryptoService;

  @BeforeEach
  void setUp() {
    httpClient = mock(HttpClient.class);
    objectMapper = new ObjectMapper(); // Use real ObjectMapper
    tokenManager = mock(ApiTokenManager.class);
    cryptoService = new ApiCryptoService(httpClient, objectMapper, tokenManager);
  }

  @Test
  void encrypt_Success() throws Exception {
    // Arrange
    String input = "010-1234-5678";
    String encrypted = "ENCRYPTED_VALUE";
    String token = "VALID_TOKEN";
    String jsonResponse = "{\"value\": [\"" + encrypted + "\"]}";

    when(tokenManager.getToken()).thenReturn(token);

    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(jsonResponse);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    // Act
    String result = cryptoService.encrypt(input);

    // Assert
    assertEquals(encrypted, result);
    verify(httpClient)
        .send(
            argThat(
                req -> {
                  return req.headers()
                      .firstValue("Authorization")
                      .orElse("")
                      .equals("Bearer " + token);
                }),
            any());
  }

  @Test
  void encrypt_401_Retry_Success() throws Exception {
    // Arrange
    String input = "010-1234-5678";
    String encrypted = "ENCRYPTED_VALUE";
    String token1 = "EXPIRED_TOKEN";
    String token2 = "NEW_TOKEN";

    when(tokenManager.getToken()).thenReturn(token1, token2);

    HttpResponse<String> response401 = mock(HttpResponse.class);
    when(response401.statusCode()).thenReturn(401);

    HttpResponse<String> response200 = mock(HttpResponse.class);
    when(response200.statusCode()).thenReturn(200);
    when(response200.body()).thenReturn("{\"value\": [\"" + encrypted + "\"]}");

    // First call returns 401, Second call returns 200
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response401, response200);

    // Act
    String result = cryptoService.encrypt(input);

    // Assert
    assertEquals(encrypted, result);
    verify(tokenManager).forceRefresh(); // Must be called
  }

  @Test
  void encrypt_Failure_500() throws Exception {
    // Arrange
    when(tokenManager.getToken()).thenReturn("TOKEN");

    HttpResponse<String> response500 = mock(HttpResponse.class);
    when(response500.statusCode()).thenReturn(500);
    when(response500.body()).thenReturn("Server Error");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response500);

    // Act
    String result = cryptoService.encrypt("test");

    // Assert
    assertNull(result); // Should be null as exception is caught and logged
  }

  @Test
  void decrypt_Success() throws Exception {
    // Arrange
    String input = "ENCRYPTED_VALUE";
    String decrypted = "010-1234-5678";
    String token = "VALID_TOKEN";
    String jsonResponse = "{\"value\": [\"" + decrypted + "\"]}";

    when(tokenManager.getToken()).thenReturn(token);

    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(jsonResponse);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    // Act
    String result = cryptoService.decrypt(input);

    // Assert
    assertEquals(decrypted, result);
    verify(httpClient)
        .send(
            argThat(
                req -> {
                  return req.headers()
                      .firstValue("Authorization")
                      .orElse("")
                      .equals("Bearer " + token);
                }),
            any());
  }

  @Test
  void encrypt_Batch_Success() throws Exception {
    // Arrange
    List<String> inputs = List.of("v1", "v2");
    String token = "VALID_TOKEN";
    // Mock response for 2 items
    String jsonResponse = "{\"value\": [\"e1\", \"e2\"]}";

    when(tokenManager.getToken()).thenReturn(token);

    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(jsonResponse);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    // Act
    List<String> results = cryptoService.encrypt(inputs);

    // Assert
    assertEquals(2, results.size());
    assertEquals("e1", results.get(0));
    assertEquals("e2", results.get(1));
  }

  @Test
  void decrypt_Batch_Success() throws Exception {
    // Arrange
    List<String> inputs = List.of("e1", "e2", "e3");
    String token = "VALID_TOKEN";
    String jsonResponse = "{\"value\": [\"v1\", \"v2\", \"v3\"]}";

    when(tokenManager.getToken()).thenReturn(token);

    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(jsonResponse);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    // Act
    List<String> results = cryptoService.decrypt(inputs);

    // Assert
    assertEquals(3, results.size());
    assertEquals("v1", results.get(0));
    assertEquals("v3", results.get(2));
  }

  @Test
  void batch_EmptyInput_ReturnsEmpty() {
    // Act
    List<String> results = cryptoService.encrypt(List.of());
    // Assert
    assertTrue(results.isEmpty());
  }
}
