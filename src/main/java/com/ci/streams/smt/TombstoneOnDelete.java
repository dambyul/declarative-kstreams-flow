package com.ci.streams.smt;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneOnDelete<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(TombstoneOnDelete.class);
  private static final String OPERATION_FIELD_DEFAULT = "__op";
  private static final String DELETE_VALUE_DEFAULT = "d";

  @Override
  public R apply(R record) {
    if (record.value() == null || !(record.value() instanceof Struct)) {
      return record;
    }

    final Struct value = (Struct) record.value();

    try {
      final Schema schema = value.schema();
      if (schema.field(OPERATION_FIELD_DEFAULT) != null) {
        Object opValue = value.get(OPERATION_FIELD_DEFAULT);
        if (opValue != null && opValue.toString().equalsIgnoreCase(DELETE_VALUE_DEFAULT)) {
          if (log.isWarnEnabled()) {
            log.warn(
                "Delete operation detected via SMT. Key: {}. Creating tombstone.", record.key());
          }
          return record.newRecord(
              record.topic(),
              record.kafkaPartition(),
              record.keySchema(),
              record.key(),
              null,
              null,
              record.timestamp());
        }
      }
    } catch (Exception e) {
      log.error("Error while processing record in TombstoneOnDelete SMT", e);
      return record;
    }

    return record;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
