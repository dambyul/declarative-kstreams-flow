package com.ci.streams.mapper;

import com.ci.streams.avro.SourceEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJsonDebeziumMapper
    implements RecordMapper<String, SourceEvent, String, GenericRecord> {

  protected static final ObjectMapper M = new ObjectMapper();
  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final Schema outputSchema;
  protected final Map<String, Object> params;

  protected AbstractJsonDebeziumMapper(Schema outputSchema, Map<String, Object> params) {
    this.outputSchema = outputSchema;
    this.params = params;
  }

  @Override
  public Record<String, GenericRecord> process(Record<String, SourceEvent> record) {
    SourceEvent event = record.value();

    if (event.getPayload() == null) {
      return null;
    }

    try {
      JsonNode dataNode = M.readTree(event.getPayload().toString());
      GenericRecord outputRecord = new GenericData.Record(outputSchema);

      if (dataNode.has("__op") && outputSchema.getField("__op") != null) {
        outputRecord.put("__op", dataNode.get("__op").asText());
      }

      mapJsonFields(dataNode, outputRecord);
      return record.withValue(outputRecord);

    } catch (Exception e) {
      log.error("Failed to map and process source event payload.", e);
      return null;
    }
  }

  protected void mapJsonFields(JsonNode dataNode, GenericRecord outputRecord) {
    for (FieldMappingRule<JsonNode> rule : getMappingRules()) {
      rule.apply(dataNode, outputRecord);
    }
  }

  protected abstract List<FieldMappingRule<JsonNode>> getMappingRules();
}
