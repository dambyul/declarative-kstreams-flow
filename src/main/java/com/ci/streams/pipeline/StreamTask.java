package com.ci.streams.pipeline;

import com.ci.streams.config.PipelineConfig;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;

/** 스트림 태스크 인터페이스. 파이프라인 설정을 기반으로 Kafka Streams 토폴로지를 구성하는 메서드를 정의합니다. */
public interface StreamTask {
  /**
   * 파이프라인 빌드.
   *
   * @param builder StreamsBuilder
   * @param streamsProps Kafka Streams 속성
   * @param config 파이프라인 설정
   */
  void buildPipeline(StreamsBuilder builder, Properties streamsProps, PipelineConfig config);
}
