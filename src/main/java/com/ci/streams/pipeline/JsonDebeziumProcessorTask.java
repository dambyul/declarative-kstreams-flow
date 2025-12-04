package com.ci.streams.pipeline;

import com.ci.streams.avro.FailRecord;
import com.ci.streams.avro.SourceEvent;
import com.ci.streams.config.PipelineDefinition;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON 형식의 Debezium 이벤트를 처리하는 태스크. JSON 데이터를 파싱하여 Debezium 구조(payload, before, after)를 처리합니다. */
public class JsonDebeziumProcessorTask extends BaseDebeziumTask<String> {

  private static final Logger log = LoggerFactory.getLogger(JsonDebeziumProcessorTask.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonDebeziumProcessorTask(String processorType) {
    super(processorType);
  }

  @Override
  protected KStream<String, GenericRecord> createInitialStream(
      StreamsBuilder builder,
      String sourceTopic,
      PipelineDefinition params,
      Properties streamsProps) {

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
}
