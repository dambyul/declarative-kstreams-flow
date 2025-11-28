package com.ci.streams.mapper;

import com.ci.streams.avro.FailRecord;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAvroKStreamsMapper
    implements RecordMapper<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final Schema outputSchema;
  protected final Map<String, Object> params;

  protected AbstractAvroKStreamsMapper(Schema outputSchema, Map<String, Object> params) {
    this.outputSchema = outputSchema;
    this.params = params;
  }

  @Override
  public Record<GenericRecord, GenericRecord> process(Record<GenericRecord, GenericRecord> record) {
    GenericRecord inputRecord = record.value();
    if (inputRecord == null) {
      return record.withValue(null);
    }

    try {
      GenericRecord outputRecord = new GenericData.Record(outputSchema);
      mapFields(inputRecord, outputRecord);
      return record.withValue(outputRecord);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Failed to map record: {}", record.value(), e);
      }
      FailRecord failRecord =
          FailRecord.newBuilder()
              .setPayload(record.value().toString())
              .setReason(e.getMessage())
              .setFailedAt(Instant.now())
              .setErrorContext("field_mapping")
              .build();
      return record.withValue(failRecord);
    }
  }

  protected void mapFields(GenericRecord inputRecord, GenericRecord outputRecord) {

    if (outputRecord.getSchema().getField("__op") != null) {
      new FieldMappingRule<>(
              "__op",
              (GenericRecord input) -> {
                if (input.getSchema().getField("__op") != null) {
                  return input.get("__op");
                }
                return null;
              })
          .apply(inputRecord, outputRecord);
    }

    for (FieldMappingRule<GenericRecord> rule : getMappingRules()) {
      rule.apply(inputRecord, outputRecord);
    }
  }

  protected Object safeGet(GenericRecord input, String fieldName) {
    if (input == null || input.getSchema().getField(fieldName) == null) {
      return null;
    }
    return input.get(fieldName);
  }

  protected abstract List<FieldMappingRule<GenericRecord>> getMappingRules();
}
