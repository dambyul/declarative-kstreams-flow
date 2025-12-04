package com.ci.streams.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** 시간 유틸리티 클래스. */
public class TimeUtils {

  private static final int KST_OFFSET_HOURS = 9;

  /** 현재 시간에 KST 오프셋(9시간)을 더한 타임스탬프 반환. */
  public static long getProcessedAtTimestamp() {
    return Instant.now().plus(KST_OFFSET_HOURS, ChronoUnit.HOURS).toEpochMilli();
  }
}
