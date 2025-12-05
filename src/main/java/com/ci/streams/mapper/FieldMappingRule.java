package com.ci.streams.mapper;

import com.ci.streams.util.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;

/** 필드 매핑 규칙 클래스. 입력 데이터에서 값을 추출하여 출력 레코드의 필드에 매핑하는 규칙을 정의합니다. */
public class FieldMappingRule<T> {

  private final String outputFieldName;
  private final Function<T, Object> valueProvider;

  public FieldMappingRule(String outputFieldName, Function<T, Object> valueProvider) {
    this.outputFieldName = outputFieldName;
    this.valueProvider = valueProvider;
  }

  public static FieldMappingRule<JsonNode> ofJsonString(String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName, dataNode -> dataNode.path(jsonPath).asText(null));
  }

  public static FieldMappingRule<JsonNode> ofJsonCleansedName(
      String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName,
        dataNode -> StreamUtils.funcDataCleansing("name", dataNode.path(jsonPath).asText(null)));
  }

  public static FieldMappingRule<JsonNode> ofJsonCleansedPhone(
      String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName,
        dataNode -> StreamUtils.funcDataCleansing("tel_no", dataNode.path(jsonPath).asText(null)));
  }

  public static FieldMappingRule<JsonNode> ofJsonCleansedEmail(
      String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName,
        dataNode -> StreamUtils.funcDataCleansing("email", dataNode.path(jsonPath).asText(null)));
  }

  public static FieldMappingRule<JsonNode> ofJsonCleansedAddress(
      String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName,
        dataNode -> StreamUtils.funcDataCleansing("address", dataNode.path(jsonPath).asText(null)));
  }

  public static FieldMappingRule<JsonNode> ofJsonTimestamp(
      String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName, dataNode -> StreamUtils.parseTimestamp(dataNode.path(jsonPath)));
  }

  public static FieldMappingRule<JsonNode> ofJsonBoolean(String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName, dataNode -> StreamUtils.ynToBoolean(dataNode, jsonPath));
  }

  public static FieldMappingRule<JsonNode> ofJsonBooleanDefault(
      String outputFieldName, String jsonPath, boolean defaultValue) {
    return new FieldMappingRule<>(
        outputFieldName, dataNode -> StreamUtils.asBoolean(dataNode, jsonPath, defaultValue));
  }

  public static FieldMappingRule<JsonNode> ofJsonInstant(String outputFieldName, String jsonPath) {
    return new FieldMappingRule<>(
        outputFieldName,
        dataNode -> StreamUtils.parseInstant(dataNode.path(jsonPath).asText(null)));
  }

  public static FieldMappingRule<GenericRecord> ofAvroString(
      String outputFieldName, String inputFieldName) {
    return new FieldMappingRule<>(
        outputFieldName,
        record -> {
          Object val = safeGetAvro(record, inputFieldName);
          return val != null ? val.toString() : null;
        });
  }

  public static FieldMappingRule<GenericRecord> ofAvroBoolean(
      String outputFieldName, String inputFieldName) {
    return new FieldMappingRule<>(outputFieldName, record -> safeGetAvro(record, inputFieldName));
  }

  public static FieldMappingRule<GenericRecord> ofAvroLong(
      String outputFieldName, String inputFieldName) {
    return new FieldMappingRule<>(outputFieldName, record -> safeGetAvro(record, inputFieldName));
  }

  public static FieldMappingRule<GenericRecord> ofAvroChannel(
      String outputFieldName, String defaultChannel) {
    return new FieldMappingRule<>(
        outputFieldName, record -> StreamUtils.resolveChannel(defaultChannel, record));
  }

  private static Object safeGetAvro(GenericRecord record, String fieldName) {
    if (record == null || record.getSchema().getField(fieldName) == null) {
      return null;
    }
    return record.get(fieldName);
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void apply(T input, GenericRecord outputRecord) {
    try {
      Object value = valueProvider.apply(input);
      if (value != null) {
        outputRecord.put(outputFieldName, value);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to apply mapping rule for field '"
              + this.outputFieldName
              + "': "
              + e.getMessage(),
          e);
    }
  }
}
