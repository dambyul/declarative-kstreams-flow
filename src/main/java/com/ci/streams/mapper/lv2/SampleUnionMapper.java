package com.ci.streams.mapper.lv2;

import static com.ci.streams.mapper.FieldMappingRule.ofAvroLong;
import static com.ci.streams.mapper.FieldMappingRule.ofAvroString;

import com.ci.streams.mapper.AbstractAvroKStreamsMapper;
import com.ci.streams.mapper.FieldMappingRule;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class SampleUnionMapper extends AbstractAvroKStreamsMapper implements Lv2Mapper {

  private final String channel;
  private final List<FieldMappingRule<GenericRecord>> MAPPING_RULES;

  public SampleUnionMapper(Schema schema, Map<String, Object> params) {
    super(schema, params);
    this.channel = (String) params.get("channel");

    this.MAPPING_RULES =
        List.of(
            ofAvroString("user_id", "cust_id"), // Lv1 User의 cust_id를 user_id로
            new FieldMappingRule<>(
                "full_name",
                input -> {
                  Object firstNameObj = safeGet(input, "cust_first_name");
                  Object lastNameObj = safeGet(input, "cust_last_name");
                  String fullName = "";
                  if (firstNameObj != null) fullName += firstNameObj.toString();
                  if (lastNameObj != null) fullName += " " + lastNameObj.toString();
                  return fullName.trim().isEmpty() ? null : fullName.trim();
                }),
            ofAvroString("contact_email", "cust_email"), // Lv1 User의 cust_email을 contact_email로
            ofAvroString("contact_phone", "cust_phone"), // Lv1 User의 cust_phone을 contact_phone로
            ofAvroString("__op", "__op"), // _op는 그대로 전달
            ofAvroLong("__processed_at", "__processed_at") // _processed_at 그대로 전달
            );
  }

  @Override
  protected List<FieldMappingRule<GenericRecord>> getMappingRules() {
    return MAPPING_RULES;
  }
}
