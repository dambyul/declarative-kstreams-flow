package com.ci.streams.config;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 파이프라인 세부 정의 클래스. 토픽, 매퍼, 스키마, 키/값 형식 등 파이프라인 실행에 필요한 모든 설정을 담고 있습니다. */
public class PipelineDefinition {
  private String name;
  private String sourceTopic;
  private String destinationTopic;
  private String mapperName;
  private String schemaName;
  private String failTopic;
  private List<String> primaryKeyFields;
  private List<String> targetTemplates;
  private String pipelineNamePrefix;
  private String pipelineNameSuffix;
  private String channel;
  private String keyType;
  private String keyFormat;
  private List<String> inputFields;
  private String inputFormat;

  private Map<String, Object> additionalProperties = new HashMap<>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSourceTopic() {
    return sourceTopic;
  }

  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  public String getDestinationTopic() {
    return destinationTopic;
  }

  public void setDestinationTopic(String destinationTopic) {
    this.destinationTopic = destinationTopic;
  }

  public String getMapperName() {
    return mapperName;
  }

  public void setMapperName(String mapperName) {
    this.mapperName = mapperName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getFailTopic() {
    return failTopic;
  }

  public void setFailTopic(String failTopic) {
    this.failTopic = failTopic;
  }

  public List<String> getPrimaryKeyFields() {
    return primaryKeyFields;
  }

  public void setPrimaryKeyFields(List<String> primaryKeyFields) {
    this.primaryKeyFields = primaryKeyFields;
  }

  public List<String> getTargetTemplates() {
    return targetTemplates;
  }

  public void setTargetTemplates(List<String> targetTemplates) {
    this.targetTemplates = targetTemplates;
  }

  public String getPipelineNamePrefix() {
    return pipelineNamePrefix;
  }

  public void setPipelineNamePrefix(String pipelineNamePrefix) {
    this.pipelineNamePrefix = pipelineNamePrefix;
  }

  public String getPipelineNameSuffix() {
    return pipelineNameSuffix;
  }

  public void setPipelineNameSuffix(String pipelineNameSuffix) {
    this.pipelineNameSuffix = pipelineNameSuffix;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getKeyType() {
    return keyType;
  }

  public void setKeyType(String keyType) {
    this.keyType = keyType;
  }

  public String getKeyFormat() {
    return keyFormat;
  }

  public void setKeyFormat(String keyFormat) {
    this.keyFormat = keyFormat;
  }

  public List<String> getInputFields() {
    return inputFields;
  }

  public void setInputFields(List<String> inputFields) {
    this.inputFields = inputFields;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String key, Object value) {
    this.additionalProperties.put(key, value);
  }
}
