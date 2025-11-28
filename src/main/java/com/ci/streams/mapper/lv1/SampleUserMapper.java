package com.ci.streams.mapper.lv1;

import static com.ci.streams.mapper.FieldMappingRule.ofJsonString;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonTimestamp;

import com.ci.streams.mapper.AbstractJsonDebeziumMapper;
import com.ci.streams.mapper.FieldMappingRule;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

public class SampleUserMapper extends AbstractJsonDebeziumMapper implements Lv1Mapper {

  private final List<FieldMappingRule<JsonNode>> MAPPING_RULES;

  public SampleUserMapper(Schema schema, Map<String, Object> params) {
    super(schema, params);

    this.MAPPING_RULES =
        List.of(
            ofJsonString("user_id", "id"),
            ofJsonString("user_name", "name"),
            ofJsonString("email", "email"),
            ofJsonTimestamp("created_at", "created_at"));
  }

  @Override
  protected List<FieldMappingRule<JsonNode>> getMappingRules() {
    return MAPPING_RULES;
  }
}
