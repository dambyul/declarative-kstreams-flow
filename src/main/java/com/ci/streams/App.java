package com.ci.streams;

import static com.ci.streams.util.Env.env;
import static com.ci.streams.util.Env.flag;
import static com.ci.streams.util.Env.must;
import static com.ci.streams.util.SecurityUtil.applySecurity;

import com.ci.streams.config.Params;
import com.ci.streams.config.PipelineConfig;
import com.ci.streams.config.PipelineGroup;
import com.ci.streams.config.Pipelines;
import com.ci.streams.pipeline.StreamTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);
  private KafkaStreams streaming;

  public static void main(String[] args) {
    App app = new App();
    Properties p = app.setProperties();
    Topology topo = app.buildTopology(p);

    if (flag("PRINT_TOPOLOGY", false)) {
      if (log.isInfoEnabled()) {
        log.info("===== TOPOLOGY =====\n{}\n====================", topo.describe());
      }
    }

    app.start(topo, p);
  }

  public Topology buildTopology(Properties p) {
    StreamsBuilder b = new StreamsBuilder();

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try (InputStream inputStream =
        this.getClass().getClassLoader().getResourceAsStream("pipelines.yml")) {
      Pipelines pipelines = mapper.readValue(inputStream, Pipelines.class);

      if (pipelines.getLv1_processors() != null) {
        processGroup(b, p, "LV1_PROCESSOR", pipelines.getLv1_processors(), mapper);
      }
      if (pipelines.getApi_processors() != null) {
        processGroup(b, p, "API_PROCESSOR", pipelines.getApi_processors(), mapper);
      }
      if (pipelines.getLv2_processors() != null) {
        processGroup(b, p, "LV2_PROCESSOR", pipelines.getLv2_processors(), mapper);
      }

    } catch (Exception e) {
      log.error("Error loading or parsing pipelines.yml or creating task", e);
      throw new RuntimeException("Failed to build topology due to configuration error", e);
    }

    return b.build();
  }

  private void processGroup(
      StreamsBuilder builder,
      Properties streamsProps,
      String type,
      PipelineGroup group,
      ObjectMapper mapper) {

    java.util.List<Map<String, Object>> allPipelines = new java.util.ArrayList<>();
    if (group.getPipelines() != null) {
      allPipelines.addAll(group.getPipelines());
    }

    if (group.getTemplates() != null && group.getSources() != null) {
      java.util.List<String> defaultTargetTemplates = null;
      if (group.getCommon() != null
          && group.getCommon().get("targetTemplates") instanceof java.util.List) {
        defaultTargetTemplates = (java.util.List<String>) group.getCommon().get("targetTemplates");
      }

      for (Map<String, Object> source : group.getSources()) {
        Object targetTemplatesObj = source.get("targetTemplates");
        java.util.List<String> targetTemplates = null;
        if (targetTemplatesObj instanceof java.util.List) {
          targetTemplates = (java.util.List<String>) targetTemplatesObj;
        } else {
          targetTemplates = defaultTargetTemplates;
        }

        for (Map<String, Object> template : group.getTemplates()) {
          if (targetTemplates != null) {
            String templateName = (String) template.get("name");
            if (templateName == null || !targetTemplates.contains(templateName)) {
              continue;
            }
          }

          Map<String, Object> generated = new HashMap<>();
          generated.putAll(template);
          generated.putAll(source);

          String prefix = (String) template.get("pipelineNamePrefix");
          String suffix = (String) source.get("pipelineNameSuffix");
          if (prefix != null && suffix != null) {
            generated.put("name", prefix + suffix);
          }
          allPipelines.add(generated);
        }
      }
    }

    Map<String, Object> commonParamsMap = group.getCommon();
    for (Map<String, Object> pipelineMap : allPipelines) {
      PipelineConfig config = new PipelineConfig();
      config.setType(type);
      config.setName((String) pipelineMap.get("name"));

      Map<String, Object> mergedParamsMap = new HashMap<>();
      if (commonParamsMap != null) {
        mergedParamsMap.putAll(commonParamsMap);
      }

      Map<String, Object> pipelineParamsMap = new HashMap<>(pipelineMap);
      pipelineParamsMap.remove("name");
      pipelineParamsMap.remove("pipelineNamePrefix");
      pipelineParamsMap.remove("pipelineNameSuffix");
      pipelineParamsMap.remove("targetTemplates");

      mergedParamsMap.putAll(pipelineParamsMap);
      mergedParamsMap.remove("targetTemplates");

      Params params = mapper.convertValue(mergedParamsMap, Params.class);
      config.setParams(params);

      StreamTask task = TaskFactory.createTask(type);
      task.buildPipeline(builder, streamsProps, config);
    }
  }

  public void start(Topology topo, Properties p) {
    streaming = new KafkaStreams(topo, p);
    streaming.setUncaughtExceptionHandler(
        e -> {
          log.error("Caught unhandled exception in stream thread, replacing thread.", e);
          return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (streaming != null) {
                    log.info("[SHUTDOWN] Closing streams...");
                    streaming.close();
                    log.info("[SHUTDOWN] Closed.");
                  }
                }));

    log.info("[BOOT] Starting KafkaStreams...");
    streaming.start();
    log.info("[BOOT] Started.");
  }

  public Properties setProperties() {
    Properties p = new Properties();
    String bs = must("KAFKA_URL").replace("kafka+ssl://", "").replace("kafka://", "");

    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, must("APPLICATION_ID"));
    p.put("schema.registry.url", must("SCHEMA_REGISTRY_URL"));
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
    p.put(StreamsConfig.STATE_DIR_CONFIG, must("STATE_DIR"));
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, env("NUM_STREAM_THREADS", "32"));
    p.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    applySecurity(p);

    return p;
  }
}
