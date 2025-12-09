package com.ci.streams.mapper.api;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ApiCustAddressInfoTest {

  private ApiCustAddressInfo apiCustAddressInfo;
  private Schema inputSchema;
  private Schema outputSchema;
  private HttpClient mockHttpClient;
  private ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  void setUp()
      throws NoSuchFieldException, IllegalAccessException, IOException, InterruptedException {
    inputSchema =
        SchemaBuilder.record("InputAddress")
            .fields()
            .name("cust_address")
            .type()
            .stringType()
            .noDefault()
            .endRecord();

    try (InputStream is =
        getClass().getClassLoader().getResourceAsStream("avro/api/ApiCustAddressInfo.avsc")) {
      if (is == null) {
        throw new IOException("Avro schema file not found: avro/ApiCustAddressInfo.avsc");
      }
      outputSchema = new Parser().parse(is);
    }

    mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse<String> mockHttpResponse = Mockito.mock(HttpResponse.class);

    ObjectNode jusoApiResult = objectMapper.createObjectNode();
    ObjectNode commonNode = objectMapper.createObjectNode();
    commonNode.put("errorCode", "0");
    commonNode.put("errorMessage", "정상");

    ArrayNode jusoArray = objectMapper.createArrayNode();
    ObjectNode jusoNode = objectMapper.createObjectNode();
    jusoNode.put("roadAddr", "서울특별시 중구 한강대로 405 (봉래동2가)");
    jusoNode.put("zipNo", "04320");
    jusoNode.put("siNm", "서울특별시");
    jusoNode.put("sggNm", "중구");
    jusoNode.put("emdNm", "봉래동2가");
    jusoArray.add(jusoNode);

    ObjectNode resultsNode = objectMapper.createObjectNode();
    resultsNode.set("common", commonNode);
    resultsNode.set("juso", jusoArray);
    jusoApiResult.set("results", resultsNode);

    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(jusoApiResult.toString());
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);

    System.setProperty("JUSO_API_KEY", "test_api_key");

    Map<String, Object> params = new HashMap<>();
    params.put("inputFields", java.util.Collections.singletonList("cust_address"));

    apiCustAddressInfo = new ApiCustAddressInfo(params, outputSchema);

    Field httpClientField = ApiCustAddressInfo.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(apiCustAddressInfo, mockHttpClient);
  }

  @Test
  void testProcessAddressInfo() {
    GenericRecord inputRecord = new GenericData.Record(inputSchema);
    inputRecord.put("cust_address", "서울 중구 한강대로 405");

    Record<String, GenericRecord> record =
        new Record<>("test-key", inputRecord, System.currentTimeMillis());

    Record<String, GenericRecord> resultRecord = apiCustAddressInfo.process(record);

    assertNotNull(resultRecord);
    assertNotNull(resultRecord.value());

    GenericRecord outputValue = resultRecord.value();

    assertEquals("서울 중구 한강대로 405", outputValue.get("full_address").toString());
    assertEquals("서울 중구 한강대로 405", outputValue.get("juso_matched_keyword").toString());
    assertEquals(1, outputValue.get("juso_result_count"));
    assertEquals("ok", outputValue.get("juso_quality").toString());
    assertEquals("서울특별시 중구 한강대로 405 (봉래동2가)", outputValue.get("road_address").toString());
    assertEquals("서울특별시", outputValue.get("city").toString());
    assertEquals("중구", outputValue.get("district").toString());
    assertEquals("봉래동2가", outputValue.get("town").toString());
    assertEquals("04320", outputValue.get("zipcode").toString());
  }

  @Test
  void testProcessWithEmptyKeyword() {
    GenericRecord inputRecord = new GenericData.Record(inputSchema);
    inputRecord.put("cust_address", "");

    Record<String, GenericRecord> record =
        new Record<>("test-key", inputRecord, System.currentTimeMillis());

    Record<String, GenericRecord> resultRecord = apiCustAddressInfo.process(record);
    assertNull(resultRecord);
  }

  @Test
  void testProcessWithNullRecordValue() {
    Record<String, GenericRecord> record =
        new Record<>("test-key", null, System.currentTimeMillis());
    Record<String, GenericRecord> resultRecord = apiCustAddressInfo.process(record);
    assertNull(resultRecord);
  }
}
