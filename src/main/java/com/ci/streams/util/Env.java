package com.ci.streams.util;

import io.github.cdimascio.dotenv.Dotenv;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class Env {
  private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

  private static final int MAX_SAMPLE_LENGTH = 256;

  private Env() {}

  public static boolean flag(final String key, final boolean def) {
    String value = System.getenv(key);
    if (value == null) {
      return def;
    }
    value = value.trim().toLowerCase();
    return "1".equals(value) || "true".equals(value) || "yes".equals(value) || "on".equals(value);
  }

  public static String env(final String key, final String defaultValue) {
    String value = dotenv.get(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      value = System.getenv(key);
    }
    return value == null || value.isBlank() ? defaultValue : value;
  }

  public static String must(final String key) {
    String value = dotenv.get(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      value = System.getenv(key);
    }
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(key + " is required");
    }
    return value;
  }

  public static String readEnvOrFile(final String key) {
    String result = null;
    String value = dotenv.get(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      value = System.getenv(key);
    }
    if (value != null && !value.isBlank()) {
      result = value;
    } else {
      String path = dotenv.get(key + "_FILE");
      if (path == null) {
        path = System.getProperty(key + "_FILE");
      }
      if (path == null) {
        path = System.getenv(key + "_FILE");
      }
      if (path != null && !path.isBlank()) {
        try {
          result = Files.readString(Paths.get(path));
        } catch (java.io.IOException e) {
          throw new IllegalStateException("Read " + key + "_FILE failed: " + path, e);
        }
      }
    }
    return result;
  }

  public static boolean notBlank(final String inputString) {
    return inputString != null && !inputString.isBlank();
  }

  public static String safe(final String inputString) {
    return inputString == null ? "null" : inputString;
  }

  public static String sample(final String inputString) {
    String result;
    if (inputString == null) {
      result = "null";
    } else if (inputString.length() <= MAX_SAMPLE_LENGTH) {
      result = inputString;
    } else {
      result = inputString.substring(0, MAX_SAMPLE_LENGTH) + "...(" + inputString.length() + "B)";
    }
    return result;
  }
}
