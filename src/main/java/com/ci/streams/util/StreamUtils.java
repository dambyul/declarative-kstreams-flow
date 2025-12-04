package com.ci.streams.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StreamUtils.class);

  private static final Pattern EMAIL_VALID_PATTERN = Pattern.compile(
      "^[a-zA-Z0-9._%+-]+[^.@]+@[A-Za-z0-9]+([.-]?[A-Za-z0-9]+)*\\.[A-Za-z]{2,}$",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern NAVER_DOMAIN_PATTERN = Pattern.compile(
      "@anaver\\.com$|@anver\\.com$|@aver\\.com$|@haver\\.com$|@nacer\\.com$|@nanver\\.com$|@narer\\.com$|"
          + "@naver\\.ccm$|@naver\\.cm$|@naver\\.cmm$|@naver\\.co$|@naver\\.cocm$|@naver\\.comet$|@naver\\.comn$|"
          + "@naver\\.con$|@naver\\.cpm$|@naver\\.cpmom$|@naver\\.dacom$|@naver\\.kr$|@naver\\.net$|@naver\\.ocm$|"
          + "@naver\\.om$|@naver\\.rocm$|@naver\\.mail$|@naver\\.vom$|@naverc\\.com$|@navercorp\\.com$|@navet\\.com$|"
          + "@navr\\.com$|@navre\\.com$|@navver\\.com$|@navwe\\.com$|@never\\.cdm$|@never\\.com$|@nzver\\.com$|"
          + "@navcer\\.com$|@naverr\\.com$|@nsver\\.com$|@nver\\.com$|@nvaer\\.com$");

  private static final Pattern NATE_DOMAIN_PATTERN = Pattern.compile(
      "@ante\\.com$|@nate\\.cim$|@nate\\.co\\.kr$|@nate\\.con$|@nate\\.conm$|@nate\\.comm$|@nate\\.net$|"
          + "@nate\\.ocm$|@natem\\.com$|@nateon\\.com$|@natel\\.com$");

  private static final Pattern DAUM_DOMAIN_PATTERN = Pattern.compile(
      "@damun\\.net$|@daum\\.com$|@daum\\.met$|@daum\\.nat$|@daum\\.co\\.kr$|@daum\\.con$|@daum\\.ner$|"
          + "@daum\\.ney$|@dauma\\.net$|@daummail\\.net$|@daun\\.net$|@duam\\.com$|@duam\\.mail$|@duam\\.net$|"
          + "@haum\\.net$|@haun\\.net$|@deum\\.net$");

  private static final Pattern GMAIL_DOMAIN_PATTERN = Pattern.compile(
      "@gail\\.com$|@gailm\\.com$|@gamail\\.com$|@gamil\\.com$|@gimai\\.com$|@gimail\\.com$|@gmai\\.com$|"
          + "@gmai\\.net$|@gmaii\\.com$|@gmail\\.cim$|@gmail\\.cocm$|@gmail\\.co\\.kr$|@gmail\\.net$|@gmail\\.som$|"
          + "@g-mail\\.com$|@gmaile\\.net$|@gmaill\\.com$|@gmaill\\.net$|@gmair\\.com$|@gmil\\.com$|@gmil\\.net$|"
          + "@gnail\\.com$|@google\\.co\\.kr$|@google\\.com$|@google\\.net$|@ngmail\\.com$|@ymail\\.com$|@googie\\.com$");

  private static final Pattern HANMAIL_DOMAIN_PATTERN = Pattern.compile(
      "@anmail\\.net$|@ghanmail\\.net$|@haamail\\.net$|@haanmail\\.net$|@hamail\\.net$|@hanmaail\\.net$|"
          + "@hanmai\\.lnet$|@hanmai\\.net$|@hanmaii\\.net$|@hanmail\\.bnet$|@hanmail\\.co\\.kr$|@hanmail\\.com$|"
          + "@hanmail\\.ent$|@hanmail\\.et$|@hanmail\\.lnet$|@hanmail\\.met$|@hanmail\\.nat$|@hanmail\\.ne$|"
          + "@hanmail\\.ner$|@hanmail\\.netm$|@hanmail\\.nt$|@hanmail\\.nwt$|@hanmail\\.vet$|@hanmaill\\.net$|"
          + "@hanmal\\.net$|@hanmali\\.net$|@hanmall\\.net$|@hanmaol\\.net$|@hanmaui\\.net$|@hanmeil\\.com$|"
          + "@hanmeil\\.net$|@hanmet\\.net$|@hanmial\\.com$|@hanmial\\.net$|@hanmiall\\.net$|@hanmil\\.net$|"
          + "@hanmmail\\.net$|@hanmsil\\.net$|@hannmail\\.net$|@hnamail\\.net$|@hnmail\\.net$|@nahmail\\.net$|"
          + "@hanmaiel\\.net$|@hanmaiji\\.net$");

  private static final Pattern OUTLOOK_DOMAIN_PATTERN = Pattern.compile(
      "@hotmail\\.co\\.kr$|@hotmail\\.de$|@hotmail\\.nat$|@hotmail\\.net$|@hotmaill\\.com$|@hotmaol\\.com$|"
          + "@outlook\\.kr$|@outiook\\.com$");

  private static final Pattern ENGLISH_PATTERN = Pattern.compile("[a-zA-Z]");
  private static final Pattern KOREAN_OR_ENGLISH_PATTERN = Pattern.compile("[a-zA-Z가-힣]");
  private static final Pattern LOWERCASE_EN_PATTERN = Pattern.compile("[a-z]");
  private static final Pattern KOREAN_PATTERN = Pattern.compile("[가-힣]");
  private static final Pattern ONLY_NON_NAME_CHARS_PATTERN = Pattern.compile("^[^a-zA-Z가-힣 ]+$");

  private StreamUtils() {
  }

  /**
   * 데이터 정제 (이메일, 전화번호, 이름, 주소 등).
   *
   * @param cleansingType 정제 유형 (email, tel_no, name, name1, address)
   * @param inputValue    원본 값
   * @return 정제된 값 (유효하지 않으면 null)
   */
  public static String funcDataCleansing(final String cleansingType, final String inputValue) {
    if (cleansingType == null || cleansingType.isEmpty())
      return null;
    if (inputValue == null || inputValue.isEmpty())
      return null;

    try {
      switch (cleansingType) {
        case "email":
          return cleanseEmail(inputValue);
        case "tel_no":
          return cleanseTelNo(inputValue);
        case "name":
          return cleanseName(inputValue);
        case "name1":
          return cleanseName1(inputValue);
        case "address":
          return cleanseAddressForFunc(inputValue);
        default:
          return null;
      }
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("funcDataCleansing error. type={}, msg={}", cleansingType, e.getMessage());
      }
      return null;
    }
  }

  private static String cleanseEmail(String inputValue) {
    String cleaned = inputValue;

    // 특수문자 제거
    cleaned = cleaned.replaceAll("[^a-zA-Z0-9._@ -]", "");
    // 연속된 점을 하나로
    cleaned = cleaned.replaceAll("\\.+", ".");
    // 소문자
    cleaned = cleaned.toLowerCase(Locale.ROOT);

    // 이메일 포맷 검증
    if (!EMAIL_VALID_PATTERN.matcher(cleaned).matches()) {
      return null;
    }

    // '(숫자)' 제거
    cleaned = cleaned.replace("(숫자)", "");

    // 도메인 오타 정규화
    cleaned = NAVER_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@naver.com");
    cleaned = NATE_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@nate.com");
    cleaned = DAUM_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@daum.net");
    cleaned = GMAIL_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@gmail.com");
    cleaned = HANMAIL_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@hanmail.net");
    cleaned = OUTLOOK_DOMAIN_PATTERN.matcher(cleaned).replaceAll("@outlook.com");

    return cleaned;
  }

  // ====== tel_no ===========================================================

  private static String cleanseTelNo(String inputValue) {
    String rst = inputValue.replaceAll("[^0-9]", "");
    int len = rst.length();

    // 9자리 : 02-XXX-XXXX
    if (len == 9 && rst.startsWith("02")) {
      return formatPhoneNumber(rst, 2, 3);
    }

    // 10자리
    if (len == 10) {
      String prefix2 = rst.substring(0, 2);
      String prefix3 = rst.substring(0, 3);

      // 02-XXXX-XXXX
      if ("02".equals(prefix2)) {
        return formatPhoneNumber(rst, 2, 4);
      }

      // 지역번호, 구 휴대폰, 050/070
      if (isLocalAreaCode(prefix3)
          || isOldMobile(prefix3)
          || "050".equals(prefix3)
          || "070".equals(prefix3)) {
        return formatPhoneNumber(rst, 3, 3);
      }

      return null;
    }

    // 11자리
    if (len == 11) {
      String prefix3 = rst.substring(0, 3);

      // 지역번호, 010, 050/070, 구 휴대폰
      if (isLocalAreaCode(prefix3)
          || "010".equals(prefix3)
          || "050".equals(prefix3)
          || "070".equals(prefix3)
          || isOldMobile(prefix3)) {
        return formatPhoneNumber(rst, 3, 4);
      }

      return null;
    }

    return null;
  }

  private static String formatPhoneNumber(String rst, int p1Len, int p2Len) {
    String p1 = rst.substring(0, p1Len);
    String p2 = rst.substring(p1Len, p1Len + p2Len);
    String p3 = rst.substring(p1Len + p2Len);
    return p1 + "-" + p2 + "-" + p3;
  }

  private static boolean isLocalAreaCode(String prefix3) {
    return "032".equals(prefix3)
        || "042".equals(prefix3)
        || "051".equals(prefix3)
        || "052".equals(prefix3)
        || "053".equals(prefix3)
        || "062".equals(prefix3)
        || "064".equals(prefix3)
        || "031".equals(prefix3)
        || "033".equals(prefix3)
        || "041".equals(prefix3)
        || "043".equals(prefix3)
        || "054".equals(prefix3)
        || "055".equals(prefix3)
        || "061".equals(prefix3)
        || "063".equals(prefix3);
  }

  private static boolean isOldMobile(String prefix3) {
    return "011".equals(prefix3)
        || "016".equals(prefix3)
        || "017".equals(prefix3)
        || "018".equals(prefix3)
        || "019".equals(prefix3);
  }

  private static String cleanseName(String inputValue) {
    String cleaned = inputValue.trim();

    // 영어가 없는 경우: 공백/슬래시 기준 잘라서 콤마 처리
    if (!ENGLISH_PATTERN.matcher(cleaned).find()) {
      cleaned = cleaned.replaceAll("\\s+", " ");
      cleaned = cleaned.replace("/", ",");

      // Heuristic:
      // 1. "홍 길 동" (all parts len=1) -> "홍길동"
      // 2. "홍길동 김철수" (any part len>1) -> "홍길동" (take first)
      if (cleaned.contains(" ")) {
        String[] parts = cleaned.split(" ");
        boolean allSingleChar = true;
        for (String p : parts) {
          if (p.length() > 1) {
            allSingleChar = false;
            break;
          }
        }

        if (allSingleChar) {
          cleaned = cleaned.replace(" ", "");
        } else {
          cleaned = parts[0];
        }
      }
    }

    // ,가 있으면 첫번째 토큰만
    if (cleaned.contains(",")) {
      String[] parts = cleaned.split(",", 2);
      cleaned = parts[0];
    } else {
      // 아니면 '(' 앞까지만
      String[] parts = inputValue.split("\\(", 2);
      cleaned = parts[0];
    }

    // 한글/영어가 포함된 경우만 정제
    if (KOREAN_OR_ENGLISH_PATTERN.matcher(cleaned).find()) {
      // 한글/영어/공백만 허용
      cleaned = cleaned.replaceAll("[^a-zA-Z가-힣 ]", "");

      // 소문자 있으면 전부 대문자로
      if (LOWERCASE_EN_PATTERN.matcher(cleaned).find()) {
        cleaned = cleaned.toUpperCase(Locale.ROOT).trim();
      }

      // 한글이 하나라도 있으면 공백 제거 (홍 길 동 → 홍길동)
      if (KOREAN_PATTERN.matcher(cleaned).find()) {
        cleaned = cleaned.replace(" ", "");
      }
    }

    if (cleaned == null
        || cleaned.trim().isEmpty()
        || ONLY_NON_NAME_CHARS_PATTERN.matcher(cleaned).matches()) {
      cleaned = "고객님";
    }

    return cleaned.trim();
  }

  /** 이름 정제 (버전 2). 공백 제거 및 특수문자/괄호 내용 삭제. */
  private static String cleanseName1(String inputValue) {
    String cleaned = inputValue;

    // 공백 → 하나의 공백
    cleaned = cleaned.replaceAll("\\s+", " ");
    // 그리고 전부 제거
    cleaned = cleaned.replace(" ", "");

    // 괄호 안 내용 제거
    cleaned = cleaned.replaceAll("\\([^)]*\\)", "");

    // 한글/영어/숫자/공백만
    cleaned = cleaned.replaceAll("[^a-zA-Z가-힣0-9 ]", "");

    if (cleaned.isEmpty()) {
      cleaned = "고객";
    }

    return cleaned;
  }

  /** 주소 정제 (함수용). 탭, NBSP 제거. */
  private static String cleanseAddressForFunc(String inputValue) {
    String cleaned = inputValue;

    // 탭 제거
    cleaned = cleaned.replace("\t", "");
    // NBSP 제거
    cleaned = cleaned.replace("\u00A0", "");

    cleaned = cleaned.trim();

    if (cleaned.length() < 2) {
      return null;
    }
    return cleaned;
  }

  /** JsonNode에서 텍스트 값 추출. */
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

  /** Y/N 값을 Boolean으로 변환. */
  public static Boolean ynToBoolean(final JsonNode dataNode, final String fieldName) {
    final JsonNode fieldNode = dataNode.path(fieldName);
    final String text = fieldNode.asText(null);
    if (text == null || text.isEmpty())
      return false;
    return "Y".equalsIgnoreCase(text.trim()) || "true".equalsIgnoreCase(text.trim());
  }

  /** null을 빈 문자열로 변환. */
  public static String nullToEmpty(final String inputString) {
    return inputString == null ? "" : inputString;
  }

  /** 현재 시간을 Epoch Milli로 반환. */
  public static Long nowEpochMilli() {
    return Instant.now().toEpochMilli();
  }

  /** 타임스탬프 문자열 파싱 (ISO-8601). */
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

  /** JsonNode에서 타임스탬프 파싱 (숫자 또는 문자열). */
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

  /** 성별 코드 정제 (M/W). */
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
    if ("남자".equals(raw)) {
      return "M";
    } else if ("여자".equals(raw)) {
      return "W";
    }
    return null;
  }

  /** 생년월일 정제 및 유효성 검사. */
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

  /** 값을 Boolean으로 변환 (Y/N, 1/0, true/false). */
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

  /** 생년월일을 Epoch Day로 변환. */
  public static Integer refineBirthToEpochDays(final String birth) {
    LocalDate date = refineBirth(birth);
    if (date == null) {
      return null;
    }
    return (int) date.toEpochDay();
  }

  /** Debezium 시퀀스 디코딩 (Base64 -> BigInteger). */
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
