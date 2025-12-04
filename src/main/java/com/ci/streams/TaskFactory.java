package com.ci.streams;

import com.ci.streams.config.PipelineConfig;
import com.ci.streams.pipeline.AvroDebeziumProcessorTask;
import com.ci.streams.pipeline.AvroKStreamsProcessorTask;
import com.ci.streams.pipeline.JsonDebeziumProcessorTask;
import com.ci.streams.pipeline.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 파이프라인 유형에 따라 적절한 StreamTask를 생성하는 팩토리 클래스. */
public class TaskFactory {
  private static final Logger log = LoggerFactory.getLogger(TaskFactory.class);

  public static StreamTask createTask(String type, PipelineConfig config) {
    log.debug("Creating task for type: {}", type);

    switch (type) {
      case "LV1_PROCESSOR":
        // 입력 포맷에 따라 JSON 또는 Avro 처리기 생성
        String inputFormat = "JSON";
        if (config != null
            && config.getParams() != null
            && config.getParams().getInputFormat() != null) {
          inputFormat = config.getParams().getInputFormat().toUpperCase();
        }

        if ("AVRO".equals(inputFormat)) {
          return new AvroDebeziumProcessorTask(type);
        } else {
          return new JsonDebeziumProcessorTask(type);
        }
      case "API_PROCESSOR":
      case "LV2_PROCESSOR":
        return new AvroKStreamsProcessorTask(type);
      default:
        throw new RuntimeException("Unknown task type: " + type);
    }
  }
}
