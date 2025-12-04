package com.ci.streams.pipeline;

import com.ci.streams.config.PipelineConfig;
import com.ci.streams.config.PipelineDefinition;
import com.ci.streams.util.SerdeFactory;
import com.ci.streams.util.TimeUtils;
import java.util.List;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 추상 프로세서 태스크 클래스. 스트림 태스크의 공통 흐름(초기화, 처리, 종료)을 정의합니다. */
public abstract class AbstractProcessorTask<K> implements StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProcessorTask.class);

  protected AbstractProcessorTask() {}

  /** 초기 스트림 생성. 소스 토픽에서 데이터를 읽어 KStream을 생성합니다. */
  protected abstract KStream<K, GenericRecord> createInitialStream(
      StreamsBuilder builder,
      String sourceTopic,
      PipelineDefinition params,
      Properties streamsProps);

  /** 스트림 처리. 비즈니스 로직(매핑, 변환 등)을 수행합니다. */
  protected abstract KStream<K, GenericRecord> processStream(
      KStream<K, GenericRecord> stream, String taskName, PipelineDefinition params);

  /** 최종 처리 및 전송. 처리된 데이터를 대상 토픽으로 전송하거나 후처리를 수행합니다. */
  protected abstract void finalizeAndSend(
      KStream<K, GenericRecord> stream,
      String taskName,
      PipelineDefinition params,
      Properties streamsProps);

  @Override
  public void buildPipeline(
      final StreamsBuilder builder, final Properties streamsProps, final PipelineConfig config) {
    final PipelineDefinition params = config.getParams();
    final String sourceTopic = params.getSourceTopic();
    final String destinationTopic = params.getDestinationTopic();
    final String failTopic = params.getFailTopic();
    final String schemaName = params.getSchemaName();
    final List<String> primaryKeyFields = params.getPrimaryKeyFields();
    final String taskName = config.getName();

    LOG.info("Initializing pipeline for task: {}", taskName);
    LOG.info("[{}] Source Topic: {}", taskName, sourceTopic);
    LOG.info("[{}] Destination Topic: {}", taskName, destinationTopic);
    LOG.info("[{}] Fail Topic: {}", taskName, failTopic);
    LOG.info("[{}] Schema Name: {}", taskName, schemaName);
    LOG.info("[{}] Primary Key Fields: {}", taskName, primaryKeyFields);

    final KStream<K, GenericRecord> initialStream =
        createInitialStream(builder, sourceTopic, params, streamsProps);

    final KStream<K, GenericRecord> processedStream =
        processStream(initialStream, taskName, params);

    final KStream<K, GenericRecord> streamWithProcessedAt =
        processedStream.mapValues(
            value -> {
              if (value != null
                  && !(value instanceof com.ci.streams.avro.FailRecord)
                  && value.getSchema().getField("__processed_at") != null) {
                value.put("__processed_at", TimeUtils.getProcessedAtTimestamp());
              }
              return value;
            });

    final java.util.Map<String, KStream<K, GenericRecord>> branches =
        streamWithProcessedAt
            .split(org.apache.kafka.streams.kstream.Named.as("branch-" + taskName + "-"))
            .branch(
                (key, value) -> value instanceof com.ci.streams.avro.FailRecord,
                org.apache.kafka.streams.kstream.Branched.as("fail"))
            .defaultBranch(org.apache.kafka.streams.kstream.Branched.as("success"));

    final Serde<GenericRecord> genericAvroSerde =
        SerdeFactory.createGenericAvroSerde(streamsProps, false);

    branches
        .get("branch-" + taskName + "-fail")
        .peek(
            (key, value) ->
                LOG.info(
                    "[{}] Failed: Key={}, Reason={}",
                    taskName,
                    key,
                    value != null ? value.get("reason") : "Unknown"))
        .selectKey((key, value) -> key != null ? key.toString() : null)
        .to(
            failTopic,
            Produced.with(org.apache.kafka.common.serialization.Serdes.String(), genericAvroSerde));

    final KStream<K, GenericRecord> successStream =
        branches.get("branch-" + taskName + "-success").filter((key, value) -> value != null);

    finalizeAndSend(successStream, taskName, params, streamsProps);

    LOG.info("Pipeline for task {} built.", taskName);
  }
}
