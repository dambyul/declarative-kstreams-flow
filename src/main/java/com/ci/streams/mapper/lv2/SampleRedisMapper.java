package com.ci.streams.mapper.lv2;

import static com.ci.streams.mapper.FieldMappingRule.ofAvroString;

import com.ci.streams.mapper.AbstractAvroKStreamsMapper;
import com.ci.streams.mapper.FieldMappingRule;
import com.ci.streams.util.TimeUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;

public class SampleRedisMapper extends AbstractAvroKStreamsMapper implements Lv2Mapper {

  private final String channel;
  private final List<FieldMappingRule<GenericRecord>> MAPPING_RULES;

  public SampleRedisMapper(Schema schema, Map<String, Object> params) {
    super(schema, params);
    this.channel = (String) params.get("channel");

    this.MAPPING_RULES =
        List.of(
            ofAvroString("entity_id", "user_id"), // Lv1 User의 user_id를 entity_id로 매핑
            new FieldMappingRule<>(
                "data",
                input -> {
                  // user_id를 기반으로 한 고유 키 생성
                  Object userIdObj = safeGet(input, "user_id");
                  String userId = userIdObj != null ? userIdObj.toString() : "";
                  String uniqueKey = "sample.user:" + this.channel + ":" + userId;
                  long processedAt = TimeUtils.getProcessedAtTimestamp();
                  return Collections.singletonMap(uniqueKey, processedAt);
                }));
  }

  @Override
  public Record<GenericRecord, GenericRecord> process(Record<GenericRecord, GenericRecord> record) {
    GenericRecord input = record.value();
    if (input == null) {
      return record.withValue(null);
    }
    // Lv1 User 스키마에 user_id가 필수라고 가정
    Object userIdObj = safeGet(input, "user_id");
    if (userIdObj == null || userIdObj.toString().trim().isEmpty()) {
      return record.withValue(null);
    }
    return super.process(record);
  }

  @Override
  protected List<FieldMappingRule<GenericRecord>> getMappingRules() {
    return MAPPING_RULES;
  }
}
