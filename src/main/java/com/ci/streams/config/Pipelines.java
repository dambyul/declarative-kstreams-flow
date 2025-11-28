package com.ci.streams.config;

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
