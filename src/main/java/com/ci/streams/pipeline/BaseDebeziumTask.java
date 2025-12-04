package com.ci.streams.pipeline;

import com.ci.streams.config.PipelineDefinition;
import com.ci.streams.processor.GenericProcessor;
import com.ci.streams.util.SerdeFactory;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Debezium 기반 태스크의 기본 클래스. Debezium CDC 데이터를 처리하는 공통 로직을 포함합니다. */
public abstract class BaseDebeziumTask<K> extends AbstractProcessorTask<K> {

  private static final Logger log = LoggerFactory.getLogger(BaseDebeziumTask.class);
  protected final String processorType;

  protected BaseDebeziumTask(String processorType) {
    this.processorType = processorType;
  }

  @Override
  protected KStream<K, GenericRecord> processStream(
      KStream<K, GenericRecord> stream, String taskName, PipelineDefinition params) {
    final String schemaName = params.getSchemaName();
    final String mapperName = params.getMapperName();
    // GenericProcessor를 사용하여 데이터 매핑 및 변환 수행
    return stream.process(
        (ProcessorSupplier<K, GenericRecord, K, GenericRecord>)
            () -> new GenericProcessor<>(this.processorType, schemaName, mapperName, params),
        Named.as("GenericProcessor-" + taskName));
  }

  @Override
  protected void finalizeAndSend(
      KStream<K, GenericRecord> stream,
      String taskName,
      PipelineDefinition params,
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
            },
            Named.as("select-avro-key-" + taskName));

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
