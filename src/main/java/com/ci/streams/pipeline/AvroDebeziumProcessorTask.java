package com.ci.streams.pipeline;

import com.ci.streams.avro.FailRecord;
import com.ci.streams.avro.SourceEvent;
import com.ci.streams.config.PipelineDefinition;
import com.ci.streams.util.SerdeFactory;
import java.time.Instant;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Avro 형식의 Debezium 이벤트를 처리하는 태스크. Debezium CDC 데이터를 읽어 Avro 레코드로 변환하고 처리합니다. */
public class AvroDebeziumProcessorTask extends BaseDebeziumTask<GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(AvroDebeziumProcessorTask.class);

  public AvroDebeziumProcessorTask(String processorType) {
    super(processorType);
  }

  @Override
  protected KStream<GenericRecord, GenericRecord> createInitialStream(
      StreamsBuilder builder,
      String sourceTopic,
      PipelineDefinition params,
      Properties streamsProps) {

    final Serde<GenericRecord> keySerde = SerdeFactory.createGenericAvroSerde(streamsProps, true);
    final Serde<GenericRecord> valueSerde =
        SerdeFactory.createGenericAvroSerde(streamsProps, false);

    return builder.stream(sourceTopic, Consumed.with(keySerde, valueSerde))
        .mapValues(
            (readOnlyKey, value) -> {
              if (value == null) {
                return SourceEvent.newBuilder().setPayload(null).build();
              }
              try {
                String op = "c";
                if (value.getSchema().getField("op") != null) {
                  Object opObj = value.get("op");
                  if (opObj != null) {
                    op = opObj.toString();
                  }
                }

                GenericRecord payloadRecord = null;
                if ("d".equals(op)) {
                  log.info(
                      "Delete operation detected for key {}. Marking with '__op: d'", readOnlyKey);
                  if (value.getSchema().getField("before") != null) {
                    payloadRecord = (GenericRecord) value.get("before");
                  }
                  if (payloadRecord == null) {
                    log.warn(
                        "Cannot process delete for key {}: 'before' field is missing or null in Debezium event.",
                        readOnlyKey);
                    return null;
                  }
                } else {
                  if (value.getSchema().getField("after") != null) {
                    payloadRecord = (GenericRecord) value.get("after");
                  }
                  if (payloadRecord == null) {
                    log.warn(
                        "Cannot process create/update for key {}: 'after' field is missing or null in Debezium event.",
                        readOnlyKey);
                    return null;
                  }
                }

                String jsonPayload = payloadRecord.toString();

                StringBuilder sb = new StringBuilder(jsonPayload);
                int firstBrace = sb.indexOf("{");
                if (firstBrace != -1) {
                  sb.insert(firstBrace + 1, "\"__op\":\"" + op + "\",");
                } else {
                  return FailRecord.newBuilder()
                      .setPayload(jsonPayload)
                      .setReason("Invalid JSON from Avro record: " + jsonPayload)
                      .setFailedAt(Instant.now())
                      .build();
                }

                return SourceEvent.newBuilder().setPayload(sb.toString()).build();

              } catch (Exception e) {
                if (log.isErrorEnabled()) {
                  log.error(
                      "Failed to process Avro record for key {}: {}", readOnlyKey, e.getMessage());
                }
                return FailRecord.newBuilder()
                    .setPayload(value.toString())
                    .setReason("Avro processing failed: " + e.getMessage())
                    .setFailedAt(Instant.now())
                    .setErrorContext("avro_processing")
                    .build();
              }
            });
  }
}
