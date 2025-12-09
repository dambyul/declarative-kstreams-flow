package com.ci.streams.mapper.lv1;

import static com.ci.streams.mapper.FieldMappingRule.ofJsonBoolean;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonCleansedEmail;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonCleansedPhone;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonString;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonTimestamp;

import com.ci.streams.mapper.FieldMappingRule;
import com.ci.streams.mapper.JdbcDecryptMapper;
import com.ci.streams.service.ApiCryptoService;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class SampleUserMapper extends JdbcDecryptMapper implements Lv1Mapper {

  private final List<FieldMappingRule<JsonNode>> MAPPING_RULES;
  private final ApiCryptoService cryptoService;

  public SampleUserMapper(Schema schema, Map<String, Object> params) {
    this(schema, params, new ApiCryptoService());
  }

  // Testing constructor
  SampleUserMapper(Schema schema, Map<String, Object> params, ApiCryptoService cryptoService) {
    super(schema, params);
    this.cryptoService = cryptoService;

    this.MAPPING_RULES =
        List.of(
            // 기본 문자열 매핑
            ofJsonString("user_id", "id"),
            ofJsonString("user_name", "name"),

            // 정제된 이메일 매핑
            ofJsonCleansedEmail("email", "email"),

            // 정제된 전화번호 매핑
            ofJsonCleansedPhone("phone", "phone_number"),

            // Boolean 매핑 (Y/N, 1/0, true/false -> boolean)
            ofJsonBoolean("is_active", "active_yn"),

            // Timestamp 매핑 (Epoch Millis로 변환)
            ofJsonTimestamp("created_at", "created_at"),

            // ApiCryptoService 암호화 예제
            new FieldMappingRule<>("encrypted_name", null) {
              @Override
              public void apply(JsonNode source, GenericRecord target) {
                String userName = source.path("name").asText(null);
                if (userName != null) {
                  String encrypted = cryptoService.encrypt(userName);
                  if (encrypted != null) {
                    target.put("encrypted_name", encrypted);
                  }
                }
              }
            },

            // ApiCryptoService 복호화 예제
            new FieldMappingRule<>("decrypted_phone", null) {
              @Override
              public void apply(JsonNode source, GenericRecord target) {
                // 예제를 위해 phone 필드가 암호화되어 있다고 가정
                String encryptedPhone = source.path("phone_number").asText(null);
                if (encryptedPhone != null) {
                  String decrypted = cryptoService.decrypt(encryptedPhone);
                  if (decrypted != null) {
                    target.put("decrypted_phone", decrypted);
                  }
                }
              }
            },

            // 커스텀 복호화 예제 (DB 기반)
            new FieldMappingRule<>("decrypted_ssn", null) {
              @Override
              public void apply(JsonNode source, GenericRecord target) {
                String pkValue = source.path("id").asText(null);
                if (pkValue != null) {
                  // DB 키 "DI", 테이블 "USERS", 컬럼 "SSN", PK 컬럼 "ID"
                  String ssn = decryptField("DI", "USERS", "SSN", "ID", pkValue);
                  if (ssn != null) {
                    target.put("decrypted_ssn", ssn);
                  }
                }
              }
            });
  }

  @Override
  protected List<FieldMappingRule<JsonNode>> getMappingRules() {
    return MAPPING_RULES;
  }
}
