package com.ci.streams.util;

import java.util.Properties;

public final class SecurityUtil {
  private SecurityUtil() {}

  public static void applySecurity(final Properties properties) {
    final String jaas = Env.readEnvOrFile("SASL_JAAS_CONFIG");
    final String truststorePath = Env.env("KAFKA_TRUSTSTORE_PATH", null);
    final String keystorePath = Env.env("KAFKA_KEYSTORE_PATH", null);
    final String pemTrust = Env.readEnvOrFile("KAFKA_TRUSTED_CERT");
    final String pemCert = Env.readEnvOrFile("KAFKA_CLIENT_CERT");
    final String pemKey = Env.readEnvOrFile("KAFKA_CLIENT_CERT_KEY_PKCS8");

    if (jaas != null && !jaas.isBlank()) {
      properties.put("security.protocol", "SASL_SSL");
      properties.put("sasl.mechanism", "PLAIN");
      properties.put("sasl.jaas.config", jaas);
      properties.put("ssl.endpoint.identification.algorithm", "https");
    } else if (Env.notBlank(truststorePath) && Env.notBlank(keystorePath)) {
      final String truststorePass = Env.env("KAFKA_TRUSTSTORE_PASS", null);
      final String keystorePass = Env.env("KAFKA_KEYSTORE_PASS", null);

      properties.put("security.protocol", "SSL");
      properties.put("ssl.truststore.location", truststorePath);
      properties.put("ssl.truststore.password", truststorePass);
      properties.put("ssl.truststore.type", "JKS");
      properties.put("ssl.keystore.location", keystorePath);
      properties.put("ssl.keystore.password", keystorePass);
      properties.put("ssl.keystore.type", "PKCS12");
      properties.put("ssl.endpoint.identification.algorithm", "");
    } else if (Env.notBlank(pemTrust) && Env.notBlank(pemCert) && Env.notBlank(pemKey)) {
      properties.put("security.protocol", "SSL");
      properties.put("ssl.truststore.type", "PEM");
      properties.put("ssl.truststore.certificates", normalizePem(pemTrust));
      properties.put("ssl.keystore.type", "PEM");
      properties.put("ssl.keystore.certificate.chain", normalizePem(pemCert));
      properties.put("ssl.keystore.key", normalizePem(pemKey));
      properties.put("ssl.endpoint.identification.algorithm", "");
    }
  }

  public static String securityModeSummary() {
    String result = "PLAINTEXT";
    if (Env.notBlank(System.getenv("SASL_JAAS_CONFIG"))) {
      result = "SASL_SSL";
    } else if (Env.notBlank(System.getenv("KAFKA_TRUSTED_CERT"))
        && Env.notBlank(System.getenv("KAFKA_CLIENT_CERT"))
        && Env.notBlank(System.getenv("KAFKA_CLIENT_CERT_KEY_PKCS8"))) {
      result = "SSL(PEM)";
    }
    return result;
  }

  public static String normalizePem(final String pem) {
    if (pem == null) {
      return null;
    }
    String tempPem = pem.replace("\r\n", "\n");
    tempPem = tempPem.replace("\r", "\n");
    tempPem = tempPem.trim();

    if (!tempPem.isEmpty() && tempPem.charAt(0) == '\uFEFF') {
      tempPem = tempPem.substring(1);
    }
    return tempPem;
  }
}
