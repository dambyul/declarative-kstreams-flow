package com.ci.streams.mapper;

import com.ci.streams.config.Params;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericMapperFactory {

  private static final Logger log = LoggerFactory.getLogger(GenericMapperFactory.class);
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final Map<String, String> MAPPER_PACKAGES =
      Map.of(
          "api", "com.ci.streams.mapper.api.",
          "lv1", "com.ci.streams.mapper.lv1.",
          "lv2", "com.ci.streams.mapper.lv2.");

  public Optional<RecordMapper> create(
      String mapperType, String schemaName, Schema schema, Params params) {
    String packageName = MAPPER_PACKAGES.get(mapperType);
    if (packageName == null) {
      log.error("Unknown mapper type: {}", mapperType);
      return Optional.empty();
    }

    try {
      Class<?> mapperClass = Class.forName(packageName + schemaName);
      if (RecordMapper.class.isAssignableFrom(mapperClass)) {
        for (Constructor<?> constructor : mapperClass.getConstructors()) {
          Class<?>[] paramTypes = constructor.getParameterTypes();
          try {
            if (params != null
                && paramTypes.length == 2
                && paramTypes[0] == Schema.class
                && paramTypes[1] == Map.class) {
              Map<String, Object> paramsMap = this.objectMapper.convertValue(params, Map.class);
              return Optional.of((RecordMapper) constructor.newInstance(schema, paramsMap));
            } else if (params != null
                && paramTypes.length == 2
                && paramTypes[0] == Map.class
                && paramTypes[1] == Schema.class) {
              Map<String, Object> paramsMap = this.objectMapper.convertValue(params, Map.class);
              return Optional.of((RecordMapper) constructor.newInstance(paramsMap, schema));
            } else if (paramTypes.length == 1 && paramTypes[0] == Schema.class) {
              return Optional.of((RecordMapper) constructor.newInstance(schema));
            }
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            if (log.isErrorEnabled()) {
              log.error("Error instantiating Mapper for schema {}: {}", schemaName, e.getMessage());
            }
            return Optional.empty();
          }
        }
        log.error("No suitable constructor found for {}", schemaName);
      }
    } catch (ClassNotFoundException e) {
      if (log.isErrorEnabled()) {
        log.error("Mapper class not found for schema: {}", schemaName);
      }
    }
    return Optional.empty();
  }
}
