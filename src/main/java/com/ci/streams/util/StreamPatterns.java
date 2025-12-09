package com.ci.streams.util;

import java.util.regex.Pattern;

/** 정규표현식 상수 모음. */
public final class StreamPatterns {

  private StreamPatterns() {}

  public static final Pattern EMAIL_VALID_PATTERN =
      Pattern.compile(
          "^[a-zA-Z0-9._%+-]+[^.@]+@[A-Za-z0-9]+([.-]?[A-Za-z0-9]+)*\\.[A-Za-z]{2,}$",
          Pattern.CASE_INSENSITIVE);

  public static final Pattern ENGLISH_PATTERN = Pattern.compile("[a-zA-Z]");
  public static final Pattern KOREAN_OR_ENGLISH_PATTERN = Pattern.compile("[a-zA-Z가-힣]");
  public static final Pattern LOWERCASE_EN_PATTERN = Pattern.compile("[a-z]");
  public static final Pattern KOREAN_PATTERN = Pattern.compile("[가-힣]");
  public static final Pattern ONLY_NON_NAME_CHARS_PATTERN = Pattern.compile("^[^a-zA-Z가-힣 ]+$");
}
