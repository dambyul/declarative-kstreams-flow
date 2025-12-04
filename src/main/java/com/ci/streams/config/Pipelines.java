package com.ci.streams.config;

/** 전체 파이프라인 설정의 최상위 클래스. LV1, API, LV2 등 각 프로세서 그룹별 설정을 포함합니다. */
public class Pipelines {
  private PipelineGroup lv1_processors;
  private PipelineGroup api_processors;
  private PipelineGroup lv2_processors;

  public PipelineGroup getLv1_processors() {
    return lv1_processors;
  }

  public void setLv1_processors(PipelineGroup lv1_processors) {
    this.lv1_processors = lv1_processors;
  }

  public PipelineGroup getApi_processors() {
    return api_processors;
  }

  public void setApi_processors(PipelineGroup api_processors) {
    this.api_processors = api_processors;
  }

  public PipelineGroup getLv2_processors() {
    return lv2_processors;
  }

  public void setLv2_processors(PipelineGroup lv2_processors) {
    this.lv2_processors = lv2_processors;
  }
}
