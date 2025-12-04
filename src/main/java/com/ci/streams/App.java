package com.ci.streams;

import static com.ci.streams.util.Env.env;
import static com.ci.streams.util.Env.flag;
import static com.ci.streams.util.Env.must;
import static com.ci.streams.util.SecurityUtil.applySecurity;

import com.ci.streams.config.PipelineConfig;
import com.ci.streams.config.PipelineGroup;
import com.ci.streams.config.Pipelines;
import com.ci.streams.pipeline.StreamTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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

/** Kafka Streams 애플리케이션 메인 클래스. 설정 로드, 토폴로지 구성 및 애플리케이션 실행을 담당합니다. */
public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);
  private KafkaStreams streaming;

  public static void main(String[] args) {
    App app = new App();
    Properties p = app.setProperties();
    Topology topo = app.buildTopology(p);

    // 토폴로지 출력 (디버깅용)
    if (flag("PRINT_TOPOLOGY", false)) {
      if (log.isInfoEnabled()) {
        log.info("===== TOPOLOGY =====\n{}\n====================", topo.describe());
      }
    }

    app.start(topo, p);
  }

  /** 파이프라인 설정을 기반으로 Kafka Streams 토폴로지를 생성합니다. */
  public Topology buildTopology(Properties p) {
    StreamsBuilder b = new StreamsBuilder();
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.configure(
        com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // 설정 파일 로드 (시스템 속성 -> 환경 변수 -> 클래스패스 순)
    String configFile = System.getProperty("config.file");
    if (configFile == null || configFile.isBlank()) {
      configFile = System.getenv("PIPELINES_CONFIG_FILE");
    }

    try (InputStream inputStream = getPipelinesInputStream(configFile)) {
      Pipelines pipelines = mapper.readValue(inputStream, Pipelines.class);

      // 각 프로세서 그룹별로 토폴로지 구성
      if (pipelines.getLv1_processors() != null) {
        processGroup(b, p, "LV1_PROCESSOR", pipelines.getLv1_processors());
      }
      if (pipelines.getApi_processors() != null) {
        processGroup(b, p, "API_PROCESSOR", pipelines.getApi_processors());
      }
      if (pipelines.getLv2_processors() != null) {
        processGroup(b, p, "LV2_PROCESSOR", pipelines.getLv2_processors());
      }

    } catch (Exception e) {
      log.error("Error loading or parsing pipelines.yml or creating task", e);
      throw new RuntimeException("Failed to build topology due to configuration error", e);
    }

    return b.build();
  }

  /** 설정 파일 입력 스트림을 가져옵니다. */
  private InputStream getPipelinesInputStream(String configFile) throws java.io.IOException {
    if (configFile != null && !configFile.isBlank()) {
      if (Files.exists(Paths.get(configFile))) {
        log.info("Loading configuration from external file: {}", configFile);
        return Files.newInputStream(Paths.get(configFile));
      } else {
        log.warn(
            "External configuration file not found at: {}. Falling back to classpath.", configFile);
      }
    }
    log.info("Loading configuration from classpath: pipelines.yml");
    InputStream is = this.getClass().getClassLoader().getResourceAsStream("pipelines.yml");
    if (is == null) {
      throw new java.io.FileNotFoundException("pipelines.yml not found in classpath");
    }
    return is;
  }

  /** 파이프라인 그룹을 처리하여 스트림 태스크를 생성하고 토폴로지에 추가합니다. */
  private void processGroup(
      StreamsBuilder builder, Properties streamsProps, String type, PipelineGroup group) {

    java.util.List<com.ci.streams.config.PipelineDefinition> allPipelines = new java.util.ArrayList<>();
    if (group.getPipelines() != null) {
      allPipelines.addAll(group.getPipelines());
    }

    // 템플릿과 소스를 기반으로 파이프라인 정의 생성
    if (group.getTemplates() != null && group.getSources() != null) {
      java.util.List<String> defaultTargetTemplates = null;
      if (group.getCommon() != null) {
        defaultTargetTemplates = group.getCommon().getTargetTemplates();
      }

      for (com.ci.streams.config.PipelineDefinition source : group.getSources()) {
        java.util.List<String> targetTemplates = source.getTargetTemplates();
        if (targetTemplates == null) {
          targetTemplates = defaultTargetTemplates;
        }

        for (com.ci.streams.config.PipelineDefinition template : group.getTemplates()) {
          if (targetTemplates != null) {
            String templateName = template.getName();
            if (templateName == null || !targetTemplates.contains(templateName)) {
              continue;
            }
          }

          com.ci.streams.config.PipelineDefinition generated = mergeDefinitions(template, source);

          String prefix = template.getPipelineNamePrefix();
          String suffix = source.getPipelineNameSuffix();
          if (prefix != null && suffix != null) {
            generated.setName(prefix + suffix);
          }
          allPipelines.add(generated);
        }
      }
    }

    com.ci.streams.config.PipelineDefinition commonDef = group.getCommon();

    for (com.ci.streams.config.PipelineDefinition pipelineDef : allPipelines) {
      com.ci.streams.config.PipelineDefinition mergedDef = commonDef != null ? mergeDefinitions(commonDef, pipelineDef)
          : pipelineDef;

      if (mergedDef.getName() == null) {
        throw new RuntimeException("Pipeline name is missing in group " + type);
      }

      if (mergedDef.getSourceTopic() == null) {
        throw new RuntimeException("Source topic is missing for pipeline: " + mergedDef.getName());
      }

      PipelineConfig config = new PipelineConfig();
      config.setType(type);
      config.setName(mergedDef.getName());

      config.setParams(mergedDef);

      // 태스크 생성 및 토폴로지 구성
      StreamTask task = TaskFactory.createTask(type, config);
      task.buildPipeline(builder, streamsProps, config);
    }
  }

  /** 두 파이프라인 정의를 병합합니다. (override가 base를 덮어씀) */
  private com.ci.streams.config.PipelineDefinition mergeDefinitions(
      com.ci.streams.config.PipelineDefinition base,
      com.ci.streams.config.PipelineDefinition override) {

    ObjectMapper merger = new ObjectMapper();
    Map<String, Object> baseMap = merger.convertValue(base, Map.class);
    Map<String, Object> overrideMap = merger.convertValue(override, Map.class);

    overrideMap.values().removeIf(java.util.Objects::isNull);

    baseMap.putAll(overrideMap);

    return merger.convertValue(baseMap, com.ci.streams.config.PipelineDefinition.class);
  }

  /** Kafka Streams 애플리케이션을 시작합니다. */
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

  /** Kafka Streams 속성을 설정합니다. */
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
        com.ci.streams.util.DlqExceptionHandler.class);
    p.put("dlq.topic.name", "error.kstreams");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    applySecurity(p);

    return p;
  }
}
