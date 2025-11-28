package com.ci.streams.config;

import java.util.List;

public class Params {
  private String sourceTopic;
  private String destinationTopic;
  private String schemaName;
  private String mapperName;
  private String failTopic;
  private List<String> primaryKeyFields;
  private List<String> inputFields;

  private String keyType;
  private String keyFormat;

  private String channel;

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

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getMapperName() {
    return mapperName;
  }

  public void setMapperName(String mapperName) {
    this.mapperName = mapperName;
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

  public List<String> getInputFields() {
    return inputFields;
  }

  public void setInputFields(List<String> inputFields) {
    this.inputFields = inputFields;
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

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }
}
