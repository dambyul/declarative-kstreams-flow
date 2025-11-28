package com.ci.streams.pipeline;

import com.ci.streams.config.PipelineConfig;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;

public interface StreamTask {
  void buildPipeline(StreamsBuilder builder, Properties streamsProps, PipelineConfig config);
}
