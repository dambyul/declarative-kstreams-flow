package com.ci.streams.mapper.lv1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ci.streams.avro.SourceEvent;
import com.ci.streams.service.ApiCryptoService;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SampleUserMapperTest {

  // Dummy env vars for ApiCryptoService (though we are mocking it, static
  // initializer runs)
  static {
    System.setProperty("API_ENC_URL", "http://localhost/enc");
    System.setProperty("API_AUTH_URL", "http://localhost/auth");
    System.setProperty("API_USERNAME", "testuser");
    System.setProperty("API_PASSWORD", "testpass");
  }

  private ApiCryptoService cryptoService;
  private SampleUserMapper mapper;

  @BeforeEach
  void setUp() {
    cryptoService = mock(ApiCryptoService.class);

    // Create a simple Avro schema for the output
    // Create a simple Avro schema for the output
    String schemaJson =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"User\","
            + "\"fields\":["
            + "  {\"name\":\"user_id\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"user_name\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"email\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"phone\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"is_active\",\"type\":[\"null\",\"boolean\"]},"
            + "  {\"name\":\"created_at\",\"type\":[\"null\",\"long\"]},"
            + "  {\"name\":\"encrypted_name\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"decrypted_phone\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"decrypted_ssn\",\"type\":[\"null\",\"string\"]}"
            + "]}";
    Schema schema = new Schema.Parser().parse(schemaJson);

    mapper = new SampleUserMapper(schema, Collections.emptyMap(), cryptoService);
  }

  @Test
  void testTransformCrypto() {
    // Arrange
    String jsonPayload =
        "{"
            + "\"id\":\"123\","
            + "\"name\":\"testName\","
            + "\"email\":\"test@example.com\","
            + "\"phone_number\":\"ENCRYPTED_PHONE\","
            + "\"active_yn\":\"Y\","
            + "\"created_at\":1678886400000"
            + "}";

    SourceEvent sourceEvent = mock(SourceEvent.class);
    when(sourceEvent.getPayload()).thenReturn(jsonPayload);

    Record<String, SourceEvent> inputRecord = new Record<>("key", sourceEvent, 0L);

    when(cryptoService.encrypt("testName")).thenReturn("ENCRYPTED_NAME_VALUE");
    when(cryptoService.decrypt("ENCRYPTED_PHONE")).thenReturn("010-1234-5678");

    // Act
    Record<String, GenericRecord> result = mapper.process(inputRecord);

    // Assert
    assertNotNull(result);
    GenericRecord output = result.value();

    // Check basic mappings
    assertEquals("123", output.get("user_id").toString());
    assertEquals("testName", output.get("user_name").toString());

    // Check crypto mappings
    assertEquals("ENCRYPTED_NAME_VALUE", output.get("encrypted_name").toString());
    assertEquals("010-1234-5678", output.get("decrypted_phone").toString());
  }
}
