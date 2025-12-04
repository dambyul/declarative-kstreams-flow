package com.ci.streams.config;

/** 파이프라인 설정 래퍼 클래스. 파이프라인의 유형, 이름 및 세부 정의를 포함합니다. */
public class PipelineConfig {
  private String type;
  private String name;
  private PipelineDefinition params;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public PipelineDefinition getParams() {
    return params;
  }

  public void setParams(PipelineDefinition params) {
    this.params = params;
  }
}
