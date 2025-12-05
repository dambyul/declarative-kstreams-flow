package com.ci.streams.util;

import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 역직렬화 예외 처리기 (DLQ 전송). 메시지 역직렬화 실패 시 해당 메시지를 DLQ(Dead Letter Queue) 토픽으로 전송하고
 * 처리를 계속합니다.
 */
public class DlqExceptionHandler implements DeserializationExceptionHandler {
  private static final Logger log = LoggerFactory.getLogger(DlqExceptionHandler.class);
  private KafkaProducer<byte[], byte[]> producer;
  private String dlqTopic;

  @Override
  public void configure(Map<String, ?> configs) {
    this.dlqTopic = (String) configs.get("dlq.topic.name");
    if (this.dlqTopic == null) {
      log.warn("DLQ topic not configured. Using default 'error.kstreams'");
      this.dlqTopic = "error.kstreams";
    }

    Map<String, Object> producerProps = new java.util.HashMap<>(configs);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProps.remove("application.id");

    this.producer = new KafkaProducer<>(producerProps);
    if (log.isDebugEnabled()) {
      log.debug("DlqExceptionHandler configured with DLQ topic: {}", this.dlqTopic);
    }
  }

  @Override
  public DeserializationHandlerResponse handle(
      ProcessorContext context,
      org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record,
      Exception exception) {

    if (log.isErrorEnabled()) {
      log.error(
          "Deserialization error in topic: {}, partition: {}, offset: {}. Sending to DLQ: {}",
          record.topic(),
          record.partition(),
          record.offset(),
          dlqTopic,
          exception);
    }

    try {
      ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(dlqTopic, null, record.key(), record.value(),
          record.headers());

      dlqRecord
          .headers()
          .add("original.topic", record.topic().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      dlqRecord
          .headers()
          .add(
              "exception.message",
              exception.getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8));

      producer.send(dlqRecord).get();

      return DeserializationHandlerResponse.CONTINUE;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (log.isErrorEnabled()) {
        log.error(
            "Failed to send record to DLQ due to interruption. This record will be SKIPPED to keep stream running.",
            e);
      }
      return DeserializationHandlerResponse.CONTINUE;
    } catch (java.util.concurrent.ExecutionException e) {
      if (log.isErrorEnabled()) {
        log.error(
            "Failed to send record to DLQ. This record will be SKIPPED to keep stream running.", e);
      }
      return DeserializationHandlerResponse.CONTINUE;
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error(
            "Failed to send record to DLQ. This record will be SKIPPED to keep stream running.", e);
      }
      return DeserializationHandlerResponse.CONTINUE;
    }
  }
}
