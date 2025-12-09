package com.ci.streams.util;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class StreamUtilsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testFuncDataCleansing() {
    // Email
    assertEquals("test@gmail.com", StreamUtils.funcDataCleansing("email", "test@gmail.com"));
    assertEquals("test@naver.com", StreamUtils.funcDataCleansing("email", "test@naver.com"));
    assertNull(StreamUtils.funcDataCleansing("email", "invalid-email"));

    // Phone
    assertEquals("010-1234-5678", StreamUtils.funcDataCleansing("tel_no", "01012345678"));
    assertEquals("02-123-4567", StreamUtils.funcDataCleansing("tel_no", "021234567"));
    assertEquals("070-1234-5678", StreamUtils.funcDataCleansing("tel_no", "07012345678"));
    assertNull(StreamUtils.funcDataCleansing("tel_no", "0111234567"));
    assertNull(StreamUtils.funcDataCleansing("tel_no", "123"));

    // Name
    assertEquals("홍길동", StreamUtils.funcDataCleansing("name", "홍 길 동"));
    assertEquals("JAMES", StreamUtils.funcDataCleansing("name", "james"));
    assertEquals("고객님", StreamUtils.funcDataCleansing("name", "!!!"));

    // Name1
    assertEquals("홍길동", StreamUtils.funcDataCleansing("name1", "홍 길 동"));
    assertEquals("홍길동", StreamUtils.funcDataCleansing("name1", "홍길동(VIP)"));

    // Address
    assertEquals("서울시 강남구", StreamUtils.funcDataCleansing("address", "서울시\t강남구"));
    assertNull(StreamUtils.funcDataCleansing("address", " "));
  }

  @Test
  void testGetText() {
    ObjectNode node = mapper.createObjectNode();
    node.put("field", "value");
    node.put("nullField", (String) null);
    node.put("emptyField", "");

    assertEquals("value", StreamUtils.getText(node, "field"));
    assertNull(StreamUtils.getText(node, "nullField"));
    assertNull(StreamUtils.getText(node, "emptyField"));
    assertNull(StreamUtils.getText(node, "missingField"));
  }

  @Test
  void testYnToBoolean() {
    ObjectNode node = mapper.createObjectNode();
    node.put("y", "Y");
    node.put("n", "N");
    node.put("true", "true");
    node.put("false", "false");

    assertTrue(StreamUtils.ynToBoolean(node, "y"));
    assertFalse(StreamUtils.ynToBoolean(node, "n"));
    assertTrue(StreamUtils.ynToBoolean(node, "true"));
    assertFalse(StreamUtils.ynToBoolean(node, "false"));
    assertFalse(StreamUtils.ynToBoolean(node, "missing"));
  }

  @Test
  void testNullToEmpty() {
    assertEquals("", StreamUtils.nullToEmpty(null));
    assertEquals("test", StreamUtils.nullToEmpty("test"));
  }

  @Test
  void testNowEpochMilli() {
    long now = StreamUtils.nowEpochMilli();
    assertTrue(now > 0);
  }

  @Test
  void testParseTimestamp() {
    ObjectNode node = mapper.createObjectNode();
    node.put("epoch", 1678886400000L);
    node.put("iso", "2023-03-15T13:20:00Z");

    assertEquals(1678886400000L, StreamUtils.parseTimestamp(node.get("epoch")));
    assertEquals(1678886400000L, StreamUtils.parseTimestamp(node.get("iso")));
  }

  @Test
  void testRefineBirth() {
    LocalDate date = StreamUtils.refineBirth("19900101");
    assertNotNull(date);
    assertEquals(LocalDate.of(1990, 1, 1), date);

    LocalDate date2 = StreamUtils.refineBirth("900101"); // 1990
    assertNotNull(date2);
    assertEquals(LocalDate.of(1990, 1, 1), date2);

    assertNull(StreamUtils.refineBirth("invalid"));
  }

  @Test
  void testContdateToTimestampMillis() {
    ObjectNode node = mapper.createObjectNode();
    node.put("date", "2023/01/01");

    Long ts = StreamUtils.contdateToTimestampMillis(node, "date");
    assertNotNull(ts);
  }

  @Test
  void testAsBoolean() {
    ObjectNode node = mapper.createObjectNode();
    node.put("val1", "1");
    node.put("val0", "0");

    assertTrue(StreamUtils.asBoolean(node, "val1", false));
    assertFalse(StreamUtils.asBoolean(node, "val0", true));
  }

  @Test
  void testRefineBirthToEpochDays() {
    Integer days = StreamUtils.refineBirthToEpochDays("19700101");
    assertEquals(0, days);
  }

  @Test
  void testDecodeDebeziumSeq() {
    ObjectNode node = mapper.createObjectNode();
    node.put("value", "AAAA"); // Base64 for 00 00 00

    String seq = StreamUtils.decodeDebeziumSeq(node);
    assertNotNull(seq);
  }

  @Test
  void testResolveChannel() {
    // 1. Config has priority
    assertEquals("ConfigChannel", StreamUtils.resolveChannel("ConfigChannel", null));

    // 2. Input record used if config is null
    Schema schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(
        java.util.List.of(
            new Schema.Field("channel", Schema.create(Schema.Type.STRING), null, (Object) null)));
    GenericRecord record = new GenericData.Record(schema);
    record.put("channel", "InputChannel");

    assertEquals("InputChannel", StreamUtils.resolveChannel(null, record));

    // 3. Null if neither
    assertNull(StreamUtils.resolveChannel(null, null));
  }
}
