package com.ci.streams.processor;

import com.ci.streams.mapper.GenericMapperFactory;
import com.ci.streams.mapper.RecordMapper;
import org.apache.avro.Schema;

/** 레코드 프로세서 팩토리. 설정에 따라 적절한 RecordProcessor 인스턴스를 생성합니다. */
public class RecordProcessorFactory {

  private static final GenericMapperFactory MAPPER_FACTORY = new GenericMapperFactory();

  public static <K, V> RecordProcessor<K, V> create(
      String type,
      String mapperName,
      Schema schema,
      com.ci.streams.config.PipelineDefinition params) {
    String mapperType = type.substring(0, type.indexOf("_PROCESSOR")).toLowerCase();

    RecordMapper<?, ?, ?, ?> mapper =
        (RecordMapper<?, ?, ?, ?>)
            MAPPER_FACTORY
                .create(mapperType, mapperName, schema, params)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "No mapper found for schema: "
                                + mapperName
                                + " and type: "
                                + mapperType));
    return (RecordProcessor<K, V>) new GenericRecordProcessor<>(mapper);
  }
}
