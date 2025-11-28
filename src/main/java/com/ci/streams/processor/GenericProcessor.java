package com.ci.streams.processor;

import com.ci.streams.avro.FailRecord;
import com.ci.streams.config.Params;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericProcessor<K, V> implements Processor<K, V, K, GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(GenericProcessor.class);
  private ProcessorContext<K, GenericRecord> context;
  private final String type;
  private final String schemaName;
  private final String mapperName;
  private final Params params;
  private RecordProcessor<K, V> recordProcessor;

  public GenericProcessor(String type, String schemaName, String mapperName, Params params) {
    this.type = type;
    this.schemaName = schemaName;
    this.mapperName = mapperName;
    this.params = params;
  }

  @Override
  public void init(ProcessorContext<K, GenericRecord> context) {
    this.context = context;
    String mapperType = type.substring(0, type.indexOf("_PROCESSOR")).toLowerCase();
    String resourceName = "avro/" + mapperType + "/" + schemaName + ".avsc";
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
      InputStream finalIs = is;
      if (is == null) {
        resourceName = "avro/" + schemaName + ".avsc";
        finalIs = getClass().getClassLoader().getResourceAsStream(resourceName);
      }

      if (finalIs == null) {
        throw new IOException("Avro schema file not found: " + resourceName);
      }
      Schema schema = new Schema.Parser().parse(finalIs);
      this.recordProcessor = RecordProcessorFactory.create(type, mapperName, schema, params);
    } catch (IOException e) {
      log.error("Failed to load Avro schema: {}", schemaName, e);
      throw new RuntimeException("Failed to load Avro schema", e);
    }
  }

  @Override
  public void process(Record<K, V> record) {
    if (recordProcessor == null) {
      throw new IllegalStateException("RecordProcessor not initialized.");
    }
    try {
      GenericRecord outputRecord = recordProcessor.process(record);
      if (outputRecord != null) {
        context.forward(record.withValue(outputRecord));
      }
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Error processing record: {}", record.key(), e);
      }
      FailRecord failRecord =
          FailRecord.newBuilder()
              .setPayload(record.value() != null ? record.value().toString() : "null")
              .setReason(e.getMessage())
              .setFailedAt(Instant.now())
              .build();
      context.forward(record.withValue(failRecord));
    }
  }

  @Override
  public void close() {}
}
