package com.ci.streams.config;

import java.util.List;
import java.util.Map;

public class PipelineGroup {
  private Map<String, Object> common;
  private List<Map<String, Object>> pipelines;
  private List<Map<String, Object>> templates;
  private List<Map<String, Object>> sources;

  public Map<String, Object> getCommon() {
    return common;
  }

  public void setCommon(Map<String, Object> common) {
    this.common = common;
  }

  public List<Map<String, Object>> getPipelines() {
    return pipelines;
  }

  public void setPipelines(List<Map<String, Object>> pipelines) {
    this.pipelines = pipelines;
  }

  public List<Map<String, Object>> getTemplates() {
    return templates;
  }

  public void setTemplates(List<Map<String, Object>> templates) {
    this.templates = templates;
  }

  public List<Map<String, Object>> getSources() {
    return sources;
  }

  public void setSources(List<Map<String, Object>> sources) {
    this.sources = sources;
  }
}
