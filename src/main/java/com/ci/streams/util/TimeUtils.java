package com.ci.streams.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

  private static final int KST_OFFSET_HOURS = 9;

  public static long getProcessedAtTimestamp() {
    return Instant.now().plus(KST_OFFSET_HOURS, ChronoUnit.HOURS).toEpochMilli();
  }
}
