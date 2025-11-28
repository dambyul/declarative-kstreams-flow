package com.ci.streams.pipeline;

import com.ci.streams.avro.FailRecord;
import com.ci.streams.avro.SourceEvent;
import com.ci.streams.config.Params;
import com.ci.streams.processor.GenericProcessor;
import com.ci.streams.util.SerdeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonDebeziumProcessorTask extends AbstractProcessorTask<String> {

  private static final Logger log = LoggerFactory.getLogger(JsonDebeziumProcessorTask.class);
  private final String processorType;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonDebeziumProcessorTask(String processorType) {
    this.processorType = processorType;
  }

  @Override
  protected KStream<String, GenericRecord> createInitialStream(
      StreamsBuilder builder, String sourceTopic, Params params, Properties streamsProps) {

    return builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues(
            (readOnlyKey, value) -> {
              if (value == null) {
                return SourceEvent.newBuilder().setPayload(null).build();
              }
              try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                JsonNode payload = root.get("payload");
                if (payload == null || payload.isNull()) {
                  return SourceEvent.newBuilder().setPayload(value).build();
                }

                String op = payload.has("op") ? payload.get("op").asText() : "c";

                if ("d".equals(op)) {
                  log.info(
                      "Delete operation detected for key {}. Marking with '__op: d'", readOnlyKey);
                  ObjectNode before = (ObjectNode) payload.get("before");
                  if (before == null || before.isNull()) {
                    log.warn(
                        "Cannot process delete for key {}: 'before' field is missing or null in Debezium event.",
                        readOnlyKey);
                    return null;
                  }
                  before.put("__op", op);
                  return SourceEvent.newBuilder().setPayload(before.toString()).build();
                } else {
                  ObjectNode after = (ObjectNode) payload.get("after");
                  if (after == null || after.isNull()) {
                    log.warn(
                        "Cannot process create/update for key {}: 'after' field is missing or null in Debezium event.",
                        readOnlyKey);
                    return null;
                  }
                  after.put("__op", op);
                  return SourceEvent.newBuilder().setPayload(after.toString()).build();
                }
              } catch (Exception e) {
                if (log.isErrorEnabled()) {
                  log.error(
                      "Failed to parse JSON for record with key {}: {}",
                      readOnlyKey,
                      e.getMessage());
                }
                return FailRecord.newBuilder()
                    .setPayload(value)
                    .setReason("JSON parsing failed: " + e.getMessage())
                    .setFailedAt(Instant.now())
                    .setErrorContext("json_parsing")
                    .build();
              }
            });
  }

  @Override
  protected KStream<String, GenericRecord> processStream(
      KStream<String, GenericRecord> stream, String taskName, Params params) {
    final String schemaName = params.getSchemaName();
    final String mapperName = params.getMapperName();
    return stream.process(
        () -> new GenericProcessor<>(this.processorType, schemaName, mapperName, params));
  }

  @Override
  protected void finalizeAndSend(
      KStream<String, GenericRecord> stream,
      String taskName,
      Params params,
      Properties streamsProps) {
    final String destinationTopic = params.getDestinationTopic();
    final List<String> primaryKeyFields = params.getPrimaryKeyFields();

    final Serde<GenericRecord> keySerde = SerdeFactory.createGenericAvroSerde(streamsProps, true);
    final Serde<GenericRecord> valueSerde =
        SerdeFactory.createGenericAvroSerde(streamsProps, false);

    KStream<GenericRecord, GenericRecord> avroKeyStream =
        stream.selectKey(
            (key, value) -> {
              final List<Schema.Field> keyFields =
                  primaryKeyFields.stream()
                      .map(
                          fieldName -> {
                            final Schema.Field field = value.getSchema().getField(fieldName);
                            if (field == null) {
                              throw new IllegalStateException(
                                  "Primary key field '"
                                      + fieldName
                                      + "' not found in mapped schema.");
                            }
                            return new Schema.Field(fieldName, field.schema(), field.doc(), null);
                          })
                      .collect(Collectors.toList());

              final Schema keySchema =
                  Schema.createRecord(
                      params.getSchemaName() + "Key", null, "com.ci.streams.avro.key", false);
              keySchema.setFields(keyFields);

              final GenericRecord keyRecord = new GenericData.Record(keySchema);
              for (final String fieldName : primaryKeyFields) {
                keyRecord.put(fieldName, value.get(fieldName));
              }
              return keyRecord;
            });

    avroKeyStream
        .peek(
            (k, v) -> {
              Object op = v.getSchema().getField("__op") != null ? v.get("__op") : "N/A";
              log.info(
                  "[{}] Processed: Key={}, Op={} -> Dest={}", taskName, k, op, destinationTopic);
              if (log.isDebugEnabled()) {
                log.debug("[{}] Producing to lv1 topic: key={}, value={}", taskName, k, v);
              }
            })
        .to(destinationTopic, Produced.with(keySerde, valueSerde));
  }
}
