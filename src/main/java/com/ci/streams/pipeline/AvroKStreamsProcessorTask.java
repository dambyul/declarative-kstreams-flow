package com.ci.streams.pipeline;

import com.ci.streams.config.Params;
import com.ci.streams.processor.GenericProcessor;
import com.ci.streams.util.SerdeFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
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

public class AvroKStreamsProcessorTask extends AbstractProcessorTask<GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKStreamsProcessorTask.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String processorType;

  public AvroKStreamsProcessorTask(final String processorType) {
    super();
    this.processorType = processorType;
  }

  @Override
  protected KStream<GenericRecord, GenericRecord> createInitialStream(
      StreamsBuilder builder, String sourceTopic, Params params, Properties streamsProps) {
    final Serde<GenericRecord> keySerde = SerdeFactory.createGenericAvroSerde(streamsProps, true);
    final Serde<GenericRecord> valueSerde =
        SerdeFactory.createGenericAvroSerde(streamsProps, false);
    return builder.stream(sourceTopic, Consumed.with(keySerde, valueSerde))
        .filter((key, value) -> value != null);
  }

  @Override
  protected KStream<GenericRecord, GenericRecord> processStream(
      KStream<GenericRecord, GenericRecord> stream, String taskName, Params params) {
    final String schemaName = params.getSchemaName();
    final String mapperName = params.getMapperName();
    return stream.process(
        () -> new GenericProcessor<>(this.processorType, schemaName, mapperName, params));
  }

  @Override
  protected void finalizeAndSend(
      KStream<GenericRecord, GenericRecord> stream,
      String taskName,
      Params params,
      Properties streamsProps) {
    final String destinationTopic = params.getDestinationTopic();
    final Serde<GenericRecord> valueSerde =
        SerdeFactory.createGenericAvroSerde(streamsProps, false);

    boolean isKeyString = "String".equalsIgnoreCase(params.getKeyType());

    if (isKeyString) {
      KStream<String, GenericRecord> stringKeyStream =
          stream.selectKey(
              (key, value) -> {
                if (params.getKeyFormat() != null && !params.getKeyFormat().isEmpty()) {
                  return formatString(params.getKeyFormat(), value);
                }
                final java.util.List<String> primaryKeyFields = params.getPrimaryKeyFields();
                Map<String, Object> keyMap = new HashMap<>();
                for (String fieldName : primaryKeyFields) {
                  keyMap.put(fieldName, value.get(fieldName));
                }
                try {
                  return OBJECT_MAPPER.writeValueAsString(keyMap);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to serialize key to JSON", e);
                }
              });

      stringKeyStream
          .peek((key, value) -> logSuccess(taskName, key, value, destinationTopic))
          .to(destinationTopic, Produced.with(Serdes.String(), valueSerde));

    } else {
      final Serde<GenericRecord> keySerde = SerdeFactory.createGenericAvroSerde(streamsProps, true);
      KStream<GenericRecord, GenericRecord> avroKeyStream =
          stream.selectKey(
              (key, value) -> {
                final java.util.List<String> primaryKeyFields = params.getPrimaryKeyFields();
                final String schemaName = params.getSchemaName();
                final java.util.List<Schema.Field> keyFields =
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
                    Schema.createRecord(schemaName + "Key", null, "com.ci.streams.avro.key", false);
                keySchema.setFields(keyFields);

                final GenericRecord keyRecord = new GenericData.Record(keySchema);
                for (final String fieldName : primaryKeyFields) {
                  keyRecord.put(fieldName, value.get(fieldName));
                }
                return keyRecord;
              });

      avroKeyStream
          .peek((key, value) -> logSuccess(taskName, key, value, destinationTopic))
          .to(destinationTopic, Produced.with(keySerde, valueSerde));
    }
  }

  private void logSuccess(
      String taskName, Object key, GenericRecord value, String destinationTopic) {
    Object op = value.getSchema().getField("__op") != null ? value.get("__op") : "N/A";
    LOG.info("[{}] Processed: Key={}, Op={} -> Dest={}", taskName, key, op, destinationTopic);
  }

  private String formatString(String format, GenericRecord record) {
    if (format == null) return record.toString();
    String result = format;
    for (Schema.Field field : record.getSchema().getFields()) {
      String placeholder = "{" + field.name() + "}";
      if (result.contains(placeholder)) {
        Object val = record.get(field.name());
        result = result.replace(placeholder, val != null ? val.toString() : "");
      }
    }
    return result;
  }
}
