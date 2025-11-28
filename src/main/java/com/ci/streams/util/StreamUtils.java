package com.ci.streams.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StreamUtils.class);

  private StreamUtils() {}

  public static String getText(final JsonNode dataNode, final String fieldName) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    if (fieldNode.isMissingNode() || fieldNode.isNull()) {
      return null;
    }
    String text = fieldNode.asText(null);
    if (text == null || text.trim().isEmpty() || "null".equalsIgnoreCase(text.trim())) {
      return null;
    }
    return text.trim();
  }

  public static Boolean ynToBoolean(final JsonNode dataNode, final String fieldName) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    final String text = fieldNode.asText(null);
    if (text == null || text.isEmpty()) return false;
    return "Y".equalsIgnoreCase(text.trim()) || "true".equalsIgnoreCase(text.trim());
  }

  public static String nullToEmpty(final String inputString) {
    return inputString == null ? "" : inputString;
  }

  public static String normalizePhone(final String phone) {
    return phone == null ? "" : phone.replaceAll("\\D+", "");
  }

  public static String cleansingAddress(final String address) {
    String result;
    if (address == null) {
      result = null;
    } else {
      result = address.replaceAll("[%=><\\\\[\\\\]]", "");
    }
    return result;
  }

  public static String cleansingGender(final String gender) {
    String result = null;
    if ("남자".equals(gender)) {
      result = "M";
    } else if ("여자".equals(gender)) {
      result = "W";
    }
    return result;
  }

  public static Long nowEpochMilli() {
    return Instant.now().toEpochMilli();
  }

  public static Long parseInstant(final String timestampStr) {
    Long result;
    if (timestampStr == null) {
      result = null;
    } else {
      try {
        result = Instant.parse(timestampStr).toEpochMilli();
      } catch (DateTimeParseException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Could not parse instant string: {}", timestampStr);
        }
        result = null;
      }
    }
    return result;
  }

  public static Long parseTimestamp(final JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.asLong();
    }
    String text = node.asText(null);
    if (text == null || text.isEmpty()) {
      return null;
    }
    if (text.chars().allMatch(Character::isDigit)) {
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException e) {
        return parseInstant(text);
      }
    }
    return parseInstant(text);
  }

  public static String getNormalizedPhone(final JsonNode dataNode, final String fieldName) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    final String fieldText = fieldNode.asText();
    return normalizePhone(fieldText);
  }

  public static String getCleansedGender(final JsonNode dataNode, final String fieldName) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    final String raw = fieldNode.asText(null);

    if (raw == null || raw.isEmpty()) {
      return null;
    }
    if ("1".equals(raw)) {
      return "M";
    }
    if ("2".equals(raw)) {
      return "W";
    }
    return cleansingGender(raw);
  }

  public static LocalDate refineBirth(final String birth) {
    if (birth == null) {
      return null;
    }

    try {
      String cleaned = birth.replaceAll("[^0-9]", "");
      if (cleaned.isEmpty()) {
        return null;
      }

      if (cleaned.length() > 8) {
        cleaned = cleaned.substring(0, 8);
      }

      LocalDate today = LocalDate.now();
      DateTimeFormatter ymdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

      if (cleaned.length() == 8) {
        LocalDate date;
        try {
          date = LocalDate.parse(cleaned, ymdFormatter);
        } catch (DateTimeParseException e) {
          return null;
        }

        if (date.isAfter(today)) {
          return null;
        }

        if (date.isBefore(LocalDate.of(1900, 1, 1))) {
          return null;
        }

        return date;
      }

      if (cleaned.length() == 6) {
        String yy = cleaned.substring(0, 2);
        String mmdd = cleaned.substring(2);

        int currentYear = today.getYear();

        String tempYYYY = "19";
        int year = Integer.parseInt(tempYYYY + yy);
        int age = currentYear - year;

        if (age > 90) {
          tempYYYY = "20";
          year = Integer.parseInt(tempYYYY + yy);
        }

        age = currentYear - year;
        if (age <= 0) {
          tempYYYY = "19";
          year = Integer.parseInt(tempYYYY + yy);
        }

        String fullYmd = String.format("%04d%s", year, mmdd);

        try {
          return LocalDate.parse(fullYmd, ymdFormatter);
        } catch (DateTimeParseException e) {
          return null;
        }
      }
      return null;

    } catch (Exception e) {
      return null;
    }
  }

  public static Boolean asBoolean(
      final JsonNode dataNode, final String fieldName, final boolean defaultValue) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    if (fieldNode.isMissingNode() || fieldNode.isNull()) {
      return defaultValue;
    }
    if (fieldNode.isBoolean()) {
      return fieldNode.asBoolean();
    }
    String text = fieldNode.asText().trim();
    if (text.isEmpty()) {
      return defaultValue;
    }
    if ("Y".equalsIgnoreCase(text) || "true".equalsIgnoreCase(text) || "1".equals(text)) {
      return true;
    }
    if ("N".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text) || "0".equals(text)) {
      return false;
    }
    return defaultValue;
  }

  public static Integer refineBirthToEpochDays(final String birth) {
    LocalDate date = refineBirth(birth);
    if (date == null) {
      return null;
    }
    return (int) date.toEpochDay();
  }

  public static String csmsCustType(final String custty) {
    if (custty == null || custty.isEmpty()) return null;

    if ("01".equals(custty)) return "자가";
    else if ("02".equals(custty)) return "임대";
    else if ("03".equals(custty)) return "장기임대";
    else if ("04".equals(custty)) return "후등기";
    else if ("05".equals(custty)) return "A/L";
    else if ("06".equals(custty)) return "뉴A/L";
    else if ("07".equals(custty)) return "리턴";
    else if ("08".equals(custty)) return "부분임대";
    else if ("51".equals(custty)) return "전/월세";
    else return null;
  }

  public static String decodeDebeziumSeq(final JsonNode seqNode) {
    if (seqNode == null || seqNode.isNull()) {
      return "";
    }
    String base64 = seqNode.path("value").asText(null);
    if (base64 == null || base64.isEmpty()) {
      return "";
    }
    try {
      byte[] bytes = Base64.getDecoder().decode(base64);
      BigInteger bi = new BigInteger(1, bytes);
      return bi.toString();
    } catch (IllegalArgumentException e) {
      return "";
    }
  }
}
