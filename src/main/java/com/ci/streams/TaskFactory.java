package com.ci.streams;

import com.ci.streams.pipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskFactory {
  private static final Logger log = LoggerFactory.getLogger(TaskFactory.class);

  public static StreamTask createTask(String type) {
    log.debug("Creating task for type: {}", type);
    switch (type) {
      case "LV1_PROCESSOR":
        return new JsonDebeziumProcessorTask(type);
      case "API_PROCESSOR":
        return new AvroKStreamsProcessorTask(type);
      case "LV2_PROCESSOR":
        return new AvroKStreamsProcessorTask(type);
      default:
        throw new RuntimeException("Unknown task type: " + type);
    }
  }
}
