package com.ci.streams.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** JSON 직렬화/역직렬화 클래스 (Serde). Jackson ObjectMapper를 사용하여 JSON 데이터를 처리합니다. */
public class JsonSerde<T> implements Serde<T> {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Class<T> targetClass;

  public JsonSerde(final Class<T> targetClass) {
    this.targetClass = targetClass;
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public Serializer<T> serializer() {
    return new Serializer<T>() {
      @Override
      public void configure(final Map<String, ?> configs, final boolean isKey) {}

      @Override
      public byte[] serialize(final String topic, final T data) {
        byte[] result;
        if (data == null) {
          result = new byte[0];
        } else {
          try {
            result = objectMapper.writeValueAsBytes(data);
          } catch (IOException e) {
            throw new SerializationException("Error serializing JSON message", e);
          }
        }
        return result;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return new Deserializer<T>() {
      @Override
      public void configure(final Map<String, ?> configs, final boolean isKey) {}

      @Override
      public T deserialize(final String topic, final byte[] data) {
        T result;
        if (data == null || data.length == 0) {
          result = null;
        } else {
          try {
            result = objectMapper.readValue(data, targetClass);
          } catch (IOException e) {
            throw new SerializationException("Error deserializing JSON message", e);
          }
        }
        return result;
      }

      @Override
      public void close() {}
    };
  }
}
