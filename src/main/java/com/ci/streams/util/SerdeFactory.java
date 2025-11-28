package com.ci.streams.util;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;

public final class SerdeFactory {

  private SerdeFactory() {}

  public static GenericAvroSerde createGenericAvroSerde(
      final Properties streamsProps, final boolean isKeySerde) {
    final Map<String, String> serdeConfig = new HashMap<>();
    for (Map.Entry<Object, Object> entry : streamsProps.entrySet()) {
      serdeConfig.put(entry.getKey().toString(), entry.getValue().toString());
    }

    final GenericAvroSerde serde = new GenericAvroSerde();
    serde.configure(serdeConfig, isKeySerde);
    return serde;
  }

  public static <T> Serde<T> createJsonSerde(final Class<T> clazz) {
    return new JsonSerde<>(clazz);
  }
}
