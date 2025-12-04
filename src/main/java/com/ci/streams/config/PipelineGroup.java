package com.ci.streams.config;

import java.util.List;

/** 파이프라인 그룹 클래스. 공통 설정, 개별 파이프라인, 템플릿 및 소스 정의를 그룹화하여 관리합니다. */
public class PipelineGroup {
  private PipelineDefinition common;
  private List<PipelineDefinition> pipelines;
  private List<PipelineDefinition> templates;
  private List<PipelineDefinition> sources;

  public PipelineDefinition getCommon() {
    return common;
  }

  public void setCommon(PipelineDefinition common) {
    this.common = common;
  }

  public List<PipelineDefinition> getPipelines() {
    return pipelines;
  }

  public void setPipelines(List<PipelineDefinition> pipelines) {
    this.pipelines = pipelines;
  }

  public List<PipelineDefinition> getTemplates() {
    return templates;
  }

  public void setTemplates(List<PipelineDefinition> templates) {
    this.templates = templates;
  }

  public List<PipelineDefinition> getSources() {
    return sources;
  }

  public void setSources(List<PipelineDefinition> sources) {
    this.sources = sources;
  }
}
